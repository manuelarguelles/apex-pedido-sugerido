"""
telegram_bot.py — Bot Maxi con autenticación + auditoría completa.

Flujo:
  /start           → saludo
  /start <CÓDIGO>  → activación one-time
  mensaje normal   → requiere sesión activa; registra en audit_log
  /admin <PASS>    → modo supervisor: generar códigos
"""
import logging, json, os, sqlite3, uuid, re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import telebot
from azure.ai.agents import AgentsClient
from azure.ai.agents.models import ThreadMessage
from azure.identity import AzureCliCredential

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Config ────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
AGENT_ID     = os.environ.get("MAXI_AGENT_ID", "asst_gHg467acQdfK5n2cS7ggQwmD")
ENDPOINT     = os.environ["AZURE_AI_FOUNDRY_PROJECT_ENDPOINT"]
ADMIN_PASS   = os.environ.get("ADMIN_PASSWORD", "ApexAdmin2026")
DB_PATH      = Path(__file__).parent.parent / "db" / "apex_demo.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN)

# Mapa user_id → thread_id (en memoria, reinicia con el bot)
user_threads: dict[int, str] = {}

# ── DB helpers ────────────────────────────────────────────────────────────
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def get_session(telegram_id: int):
    with db() as con:
        row = con.execute(
            "SELECT * FROM sessions WHERE telegram_id=? AND activo=1", (telegram_id,)
        ).fetchone()
    return dict(row) if row else None

def activate_code(code: str, telegram_id: int):
    """Intenta activar un código one-time. Retorna (ok, mensaje)."""
    with db() as con:
        row = con.execute(
            "SELECT * FROM activation_codes WHERE codigo=? AND estado='PENDING'", (code,)
        ).fetchone()
        if not row:
            existing = con.execute(
                "SELECT estado FROM activation_codes WHERE codigo=?", (code,)
            ).fetchone()
            if existing:
                return False, f"⚠️ Este código ya fue *{existing['estado']}*. Pide uno nuevo a tu supervisor."
            return False, "❌ Código inválido. Verifica con tu supervisor."
        if row["expires_at"] < datetime.now().isoformat():
            return False, "⏰ El código expiró. Pide uno nuevo a tu supervisor."
        # Marcar código como USED
        con.execute(
            "UPDATE activation_codes SET estado='USED', telegram_id=?, activated_at=datetime('now') WHERE codigo=?",
            (telegram_id, code)
        )
        # Crear sesión
        con.execute("DELETE FROM sessions WHERE telegram_id=?", (telegram_id,))
        con.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,1,datetime('now'))",
            (telegram_id, row["vendedor_id"], row["nombre"], "Campo")
        )
        con.commit()
    return True, row["nombre"]

def generate_code(vendedor_id: str, nombre: str) -> str:
    """Genera un código de activación único."""
    code = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    with db() as con:
        con.execute(
            "INSERT INTO activation_codes VALUES (?,?,?,'PENDING',NULL,datetime('now'),datetime('now','+48 hours'),NULL)",
            (code, vendedor_id, nombre)
        )
        con.commit()
    return code

def log_query(telegram_id: int, session: dict, cliente_id: str,
              tool: str, params: str, resumen: str, thread_id: str):
    """Registra TODA consulta en audit_log, confirmada o no."""
    with db() as con:
        con.execute(
            "INSERT INTO audit_log VALUES (?,?,?,?,?,?,?,?,datetime('now'),?)",
            (str(uuid.uuid4()), telegram_id,
             session.get("vendedor_id","?"), session.get("nombre","?"),
             cliente_id, tool, params, resumen, thread_id)
        )
        con.commit()

# ── Tool dispatcher ───────────────────────────────────────────────────────
from agent.tools import TOOL_MAP

def run_tool(name: str, args: dict) -> str:
    fn = TOOL_MAP.get(name)
    if not fn:
        return json.dumps({"error": f"Tool {name} no existe"})
    return json.dumps(fn(**args), ensure_ascii=False)

# ── Azure AI Foundry ──────────────────────────────────────────────────────
def get_maxi_response(user_id: int, user_msg: str, session: dict) -> tuple[str, str]:
    """Envía mensaje a Maxi y procesa tool calls. Retorna (respuesta, thread_id)."""
    with AgentsClient(endpoint=ENDPOINT, credential=AzureCliCredential()) as client:
        # Thread por usuario (persistente durante la sesión del bot)
        thread_id = user_threads.get(user_id)
        if not thread_id:
            thread = client.create_thread()
            thread_id = thread.id
            user_threads[user_id] = thread_id

        client.create_message(thread_id=thread_id, role="user", content=user_msg)
        run = client.create_and_process_run(thread_id=thread_id, agent_id=AGENT_ID)

        # Procesar tool calls
        while run.status in ("requires_action",):
            tool_outputs = []
            for tc in run.required_action.submit_tool_outputs.tool_calls:
                args = json.loads(tc.function.arguments)
                result = run_tool(tc.function.name, args)
                tool_outputs.append({"tool_call_id": tc.id, "output": result})
            run = client.submit_tool_outputs_to_run(
                thread_id=thread_id, run_id=run.id, tool_outputs=tool_outputs
            )

        # Obtener respuesta
        messages = list(client.list_messages(thread_id=thread_id))
        for m in messages:
            if m.role == "assistant":
                for c in m.content:
                    if hasattr(c, "text"):
                        return c.text.value, thread_id
        return "_(sin respuesta)_", thread_id

# ── Handlers ──────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
def handle_start(msg):
    uid  = msg.from_user.id
    args = msg.text.strip().split()

    # /start con código de activación
    if len(args) > 1:
        code = args[1].upper()
        ok, data = activate_code(code, uid)
        if ok:
            bot.send_message(uid,
                f"✅ *¡Bienvenido, {data}!*\n\n"
                f"Soy *Maxi* 🧃, tu asistente de pedidos del Grupo Mariposa.\n"
                f"Cuéntame qué cliente vas a visitar hoy y te preparo la sugerencia del modelo.",
                parse_mode="Markdown"
            )
            log.info(f"Nuevo rep activado: {data} (TG:{uid})")
        else:
            bot.send_message(uid, data, parse_mode="Markdown")
        return

    # /start sin código
    session = get_session(uid)
    if session:
        bot.send_message(uid,
            f"👋 Hola de nuevo, *{session['nombre']}*!\n\n"
            f"¿A qué cliente vamos a visitar hoy?",
            parse_mode="Markdown"
        )
    else:
        bot.send_message(uid,
            "🔒 *Acceso restringido*\n\n"
            "Este bot es de uso exclusivo para asesores del *Grupo Mariposa*.\n\n"
            "Si eres parte del equipo, pide tu código de activación a tu supervisor "
            "y escribe:\n`/start TU-CÓDIGO`",
            parse_mode="Markdown"
        )


@bot.message_handler(commands=["admin"])
def handle_admin(msg):
    """Supervisor genera códigos: /admin <PASS> <vendedor_id> <nombre>"""
    uid  = msg.from_user.id
    parts = msg.text.strip().split(None, 3)

    if len(parts) < 2 or parts[1] != ADMIN_PASS:
        bot.send_message(uid, "❌ Comando inválido.")
        return

    if len(parts) < 4:
        bot.send_message(uid,
            "📋 *Generar código de acceso:*\n"
            "`/admin PASS VEN001 Nombre Apellido`",
            parse_mode="Markdown"
        )
        return

    vendedor_id = parts[2]
    nombre      = parts[3]
    code        = generate_code(vendedor_id, nombre)
    bot.send_message(uid,
        f"✅ *Código generado:*\n\n"
        f"```\n{code}\n```\n\n"
        f"Rep: *{nombre}* ({vendedor_id})\n"
        f"⏰ Válido por 48 horas — uso único.",
        parse_mode="Markdown"
    )
    log.info(f"Admin generó código {code} para {nombre} ({vendedor_id})")


@bot.message_handler(commands=["auditoria"])
def handle_audit(msg):
    """Supervisor ve últimas consultas: /auditoria <PASS> [vendedor_id]"""
    parts = msg.text.strip().split(None, 2)
    if len(parts) < 2 or parts[1] != ADMIN_PASS:
        bot.send_message(msg.from_user.id, "❌ Acceso denegado.")
        return

    filtro = f"AND vendedor_id='{parts[2]}'" if len(parts) > 2 else ""
    with db() as con:
        rows = con.execute(
            f"SELECT vendedor_nombre, cliente_id, tool_llamada, resultado_resumen, timestamp "
            f"FROM audit_log {filtro} ORDER BY timestamp DESC LIMIT 10"
        ).fetchall()

    if not rows:
        bot.send_message(msg.from_user.id, "Sin consultas registradas aún.")
        return

    text = "📊 *Últimas consultas:*\n\n"
    for r in rows:
        text += (f"👤 *{r['vendedor_nombre']}* → {r['cliente_id'] or '?'}\n"
                 f"   🔧 `{r['tool_llamada']}` — {r['timestamp'][:16]}\n"
                 f"   _{r['resultado_resumen'][:80]}_\n\n")
    bot.send_message(msg.from_user.id, text, parse_mode="Markdown")


@bot.message_handler(func=lambda m: True)
def handle_message(msg):
    uid = msg.from_user.id

    # Verificar sesión
    session = get_session(uid)
    if not session:
        bot.send_message(uid,
            "🔒 No tienes acceso. Usa `/start TU-CÓDIGO` para activar tu cuenta.",
            parse_mode="Markdown"
        )
        return

    bot.send_chat_action(uid, "typing")

    try:
        # Enriquecer el mensaje con contexto del vendedor
        contexto = f"[Vendedor: {session['nombre']} | ID: {session['vendedor_id']} | Zona: {session['zona']}]\n"
        respuesta, thread_id = get_maxi_response(uid, contexto + msg.text, session)

        # Extraer cliente_id mencionado (si hay)
        cliente_match = re.search(r"CLI\d{3}", respuesta + msg.text)
        cliente_id = cliente_match.group(0) if cliente_match else None

        # SIEMPRE registrar en audit_log
        log_query(
            telegram_id=uid,
            session=session,
            cliente_id=cliente_id,
            tool="chat",
            params=msg.text[:200],
            resumen=respuesta[:200],
            thread_id=thread_id
        )

        bot.send_message(uid, respuesta, parse_mode="Markdown")

    except Exception as e:
        log.error(f"Error procesando mensaje de {uid}: {e}")
        bot.send_message(uid, "⚠️ Tuve un problema técnico. Intenta de nuevo en un momento.")


# ── Main ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🧃 Maxi Bot iniciado — esperando mensajes...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
