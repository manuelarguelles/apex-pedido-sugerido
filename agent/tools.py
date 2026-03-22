"""
tools.py — Herramientas del agente Maxi.
Databricks Premium (apex_dq_premium.pedido_sugerido) vía SQL Serverless.
Fallback automático a SQLite local si Databricks no está disponible.
"""
import sqlite3, json, os, requests
from pathlib import Path
from datetime import datetime

# ── Databricks config ─────────────────────────────────────────────────────
ADB_HOST = "https://adb-7405617419570518.18.azuredatabricks.net"
ADB_TOKEN_PATH = Path.home() / ".config/databricks/adb_prem_token"
WH_ID = "ad52a9849c1c7ebf"
CATALOG = "apex_dq_premium"
SCHEMA  = "pedido_sugerido"

# ── SQLite fallback ───────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "db" / "apex_demo.db"

def _adb_token():
    """Obtiene token del SP via client credentials (sin expiración de sesión).
    Requiere variables de entorno: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
    """
    client_id     = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
    tenant_id     = os.environ.get("AZURE_TENANT_ID", "")

    if client_id and client_secret and tenant_id:
        try:
            import requests as _req
            r = _req.post(
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "resource": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
                }, timeout=10
            )
            token = r.json().get("access_token", "")
            if token:
                return token
        except Exception:
            pass
    # Fallback: token guardado en disco
    if ADB_TOKEN_PATH.exists():
        return ADB_TOKEN_PATH.read_text().strip()
    return os.environ.get("DATABRICKS_TOKEN", "")

def _q(sql_stmt, params=()):
    """Ejecuta query: intenta Databricks primero, fallback SQLite."""
    token = _adb_token()
    if token:
        # Substituir ? por valores (Databricks no soporta ? params)
        if params:
            for p in params:
                p_str = f"'{p}'" if isinstance(p, str) else str(p)
                sql_stmt = sql_stmt.replace("?", p_str, 1)
        # Adaptar nombres de tablas al catálogo Databricks
        full_sql = sql_stmt.replace(
            " clientes", f" {CATALOG}.{SCHEMA}.clientes"
        ).replace(
            " vendedores", f" {CATALOG}.{SCHEMA}.vendedores"
        ).replace(
            " skus", f" {CATALOG}.{SCHEMA}.skus"
        ).replace(
            " historial_compras", f" {CATALOG}.{SCHEMA}.historial_compras"
        ).replace(
            " stock_actual", f" {CATALOG}.{SCHEMA}.stock_actual"
        ).replace(
            " sugerencias_modelo", f" {CATALOG}.{SCHEMA}.sugerencias_modelo"
        ).replace(
            " pedidos_confirmados", f" {CATALOG}.{SCHEMA}.pedidos_confirmados"
        )
        try:
            r = requests.post(
                f"{ADB_HOST}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {token}"},
                json={"warehouse_id": WH_ID, "statement": full_sql,
                      "wait_timeout": "30s", "on_wait_timeout": "WAIT"},
                timeout=35
            )
            d = r.json()
            if d.get("status", {}).get("state") == "SUCCEEDED":
                schema_cols = [c["name"] for c in d.get("manifest", {}).get("schema", {}).get("columns", [])]
                rows = d.get("result", {}).get("data_array", [])
                return [dict(zip(schema_cols, row)) for row in rows]
        except Exception as e:
            pass  # Fallback to SQLite
    # SQLite fallback
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(sql_stmt, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def _exec(sql_stmt, params=()):
    """INSERT/UPDATE: Databricks primero, fallback SQLite."""
    token = _adb_token()
    if token and params:
        vals = ", ".join([f"'{p}'" if isinstance(p, str) else str(p) for p in params])
        full = sql_stmt.replace("?", "{}", -1)
        for p in params:
            p_str = f"'{p}'" if isinstance(p, str) else str(p)
            full = full.replace("?", p_str, 1)
        for t in ["clientes","vendedores","skus","historial_compras","stock_actual","sugerencias_modelo","pedidos_confirmados"]:
            full = full.replace(f" {t}", f" {CATALOG}.{SCHEMA}.{t}")
        try:
            r = requests.post(
                f"{ADB_HOST}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {token}"},
                json={"warehouse_id": WH_ID, "statement": full,
                      "wait_timeout": "30s", "on_wait_timeout": "WAIT"},
                timeout=35
            )
            if r.json().get("status", {}).get("state") == "SUCCEEDED":
                return
        except Exception:
            pass
    # SQLite fallback
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(sql_stmt, params)
    con.commit()
    con.close()


def get_client_profile(cliente_query: str) -> dict:
    """Busca el perfil de un cliente por nombre o ID."""
    rows = _q("""
        SELECT c.cliente_id, c.nombre, c.tipo, c.pais, c.region,
               v.nombre as vendedor, v.zona,
               COUNT(DISTINCT h.pedido_id) as total_pedidos,
               ROUND(SUM(CAST(h.monto_total AS REAL)), 2) as ventas_totales_6m
        FROM clientes c
        JOIN vendedores v ON c.vendedor_id = v.vendedor_id
        LEFT JOIN historial_compras h ON c.cliente_id = h.cliente_id
        WHERE LOWER(c.cliente_id) = LOWER(?)
           OR LOWER(c.nombre) LIKE LOWER(?)
        GROUP BY c.cliente_id, c.nombre, c.tipo, c.pais, c.region, v.nombre, v.zona
        LIMIT 5
    """, (cliente_query, f"%{cliente_query}%"))

    if not rows:
        return {"error": f"No se encontró cliente con '{cliente_query}'. Verifica el nombre o ID."}
    return rows[0] if len(rows) == 1 else {"clientes": rows}


def get_suggested_order(cliente_id: str) -> dict:
    """Retorna la sugerencia del modelo ML para esta semana."""
    rows = _q("""
        SELECT s.sku_id, sk.nombre as sku_nombre, sk.categoria,
               CAST(s.cajas_sugeridas AS INTEGER) as cajas_sugeridas,
               CAST(s.confianza_pct AS REAL) as confianza_pct,
               CAST(s.tendencia_pct AS REAL) as tendencia_pct,
               s.razon_principal,
               CAST(sk.precio_caja AS REAL) as precio_caja,
               ROUND(CAST(s.cajas_sugeridas AS REAL) * CAST(sk.precio_caja AS REAL), 2) as valor_estimado
        FROM sugerencias_modelo s
        JOIN skus sk ON s.sku_id = sk.sku_id
        WHERE s.cliente_id = ?
          AND CAST(s.cajas_sugeridas AS INTEGER) > 0
        ORDER BY CAST(s.cajas_sugeridas AS INTEGER) DESC
    """, (cliente_id,))

    if not rows:
        return {"error": f"No hay sugerencias para el cliente {cliente_id}"}

    total_cajas = sum(int(r["cajas_sugeridas"]) for r in rows)
    total_valor = sum(float(r["valor_estimado"]) for r in rows)
    return {
        "cliente_id": cliente_id,
        "sugerencias": rows,
        "total_cajas": total_cajas,
        "total_valor_estimado": round(total_valor, 2),
        "fecha_generacion": datetime.now().strftime("%Y-%m-%d")
    }


def get_purchase_history(cliente_id: str, semanas: int = 8) -> dict:
    """Historial de compras de las últimas N semanas."""
    rows = _q("""
        SELECT sk.nombre as sku_nombre,
               COUNT(*) as pedidos,
               ROUND(AVG(CAST(h.cajas_pedidas AS REAL)), 1) as promedio_cajas,
               SUM(CAST(h.cajas_pedidas AS INTEGER)) as total_cajas
        FROM historial_compras h
        JOIN skus sk ON h.sku_id = sk.sku_id
        WHERE h.cliente_id = ?
          AND h.fecha >= DATE('now', ? || ' days')
        GROUP BY sk.nombre
        ORDER BY total_cajas DESC
    """, (cliente_id, f"-{semanas * 7}"))

    if not rows:
        return {"mensaje": f"Sin historial reciente para {cliente_id}"}
    return {"cliente_id": cliente_id, "semanas": semanas, "resumen_por_sku": rows}


def get_stock_alert(cliente_id: str) -> dict:
    """SKUs con stock bajo (0-3 cajas) en el punto de venta."""
    rows = _q("""
        SELECT sk.nombre, CAST(sa.stock_cajas AS INTEGER) as stock_cajas, sk.categoria,
               CASE WHEN CAST(sa.stock_cajas AS INTEGER) = 0 THEN 'URGENTE'
                    WHEN CAST(sa.stock_cajas AS INTEGER) <= 3 THEN 'BAJO'
                    ELSE 'OK' END as nivel
        FROM stock_actual sa
        JOIN skus sk ON sa.sku_id = sk.sku_id
        WHERE sa.cliente_id = ?
          AND CAST(sa.stock_cajas AS INTEGER) <= 3
        ORDER BY CAST(sa.stock_cajas AS INTEGER) ASC
    """, (cliente_id,))

    if not rows:
        return {"mensaje": "Stock OK en todos los SKUs — sin alertas"}

    urgentes = [r for r in rows if r["nivel"] == "URGENTE"]
    bajos    = [r for r in rows if r["nivel"] == "BAJO"]
    return {
        "cliente_id": cliente_id,
        "alertas_urgentes": urgentes,
        "alertas_bajas": bajos,
        "total_alertas": len(rows)
    }


def confirm_order(cliente_id: str, vendedor_id: str, items: list, canal: str = "telegram") -> dict:
    """Confirma y registra el pedido final."""
    pedido_id = f"CONF{int(datetime.now().timestamp())}"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_cajas = 0
    total_valor = 0.0

    for item in items:
        motivo = item.get("motivo_ajuste", "Sin ajuste")
        cajas_conf = item["cajas_confirmadas"]
        cajas_sug  = item.get("cajas_sugeridas", cajas_conf)
        _exec("""
            INSERT INTO pedidos_confirmados VALUES (?,?,?,?,?,?,?,?,?)
        """, (pedido_id, cliente_id, vendedor_id, item["sku_id"],
              cajas_conf, cajas_sug, motivo, ts, canal))

        precio_rows = _q("SELECT precio_caja FROM skus WHERE sku_id = ?", (item["sku_id"],))
        precio = float(precio_rows[0]["precio_caja"]) if precio_rows else 0
        total_cajas += cajas_conf
        total_valor += cajas_conf * precio

    return {
        "pedido_id": pedido_id,
        "cliente_id": cliente_id,
        "total_cajas": total_cajas,
        "total_valor_usd": round(total_valor, 2),
        "items_confirmados": len(items),
        "estado": "CONFIRMADO ✅",
        "timestamp": ts
    }


# ── Definiciones de tools para Azure AI Foundry ───────────────────────────

TOOL_DEFINITIONS = [
    {"type": "function", "function": {
        "name": "get_client_profile",
        "description": "Busca el perfil de un cliente por nombre o ID. Retorna tipo, región, vendedor y resumen de ventas.",
        "parameters": {"type": "object",
            "properties": {"cliente_query": {"type": "string", "description": "Nombre parcial o ID del cliente"}},
            "required": ["cliente_query"]}
    }},
    {"type": "function", "function": {
        "name": "get_suggested_order",
        "description": "Obtiene la sugerencia de pedido del modelo ML para un cliente esta semana.",
        "parameters": {"type": "object",
            "properties": {"cliente_id": {"type": "string", "description": "ID del cliente (ej: CLI001)"}},
            "required": ["cliente_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_purchase_history",
        "description": "Historial de compras del cliente en las últimas N semanas.",
        "parameters": {"type": "object",
            "properties": {
                "cliente_id": {"type": "string", "description": "ID del cliente"},
                "semanas": {"type": "integer", "description": "Semanas hacia atrás (default 8)"}
            },
            "required": ["cliente_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_stock_alert",
        "description": "Verifica stock actual. Alerta sobre SKUs con 0 cajas (urgente) o menos de 3 (bajo).",
        "parameters": {"type": "object",
            "properties": {"cliente_id": {"type": "string", "description": "ID del cliente"}},
            "required": ["cliente_id"]}
    }},
    {"type": "function", "function": {
        "name": "confirm_order",
        "description": "Confirma y registra el pedido final.",
        "parameters": {"type": "object",
            "properties": {
                "cliente_id": {"type": "string"},
                "vendedor_id": {"type": "string"},
                "items": {
                    "type": "array",
                    "description": "Lista de productos a confirmar",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku_id": {"type": "string", "description": "ID del SKU"},
                            "cajas_confirmadas": {"type": "integer", "description": "Cajas confirmadas"},
                            "cajas_sugeridas": {"type": "integer", "description": "Cajas sugeridas por el modelo"},
                            "motivo_ajuste": {"type": "string", "description": "Razón del ajuste"}
                        },
                        "required": ["sku_id", "cajas_confirmadas"]
                    }
                },
                "canal": {"type": "string", "description": "Canal de confirmación"}
            },
            "required": ["cliente_id", "vendedor_id", "items"]}
    }},
]

TOOL_MAP = {
    "get_client_profile":   get_client_profile,
    "get_suggested_order":  get_suggested_order,
    "get_purchase_history": get_purchase_history,
    "get_stock_alert":      get_stock_alert,
    "confirm_order":        confirm_order,
}
