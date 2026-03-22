"""
setup_databricks.py — Carga los CSVs del demo a Azure Databricks Premium (SQL Serverless).
"""
import csv, time, io, requests, json
from pathlib import Path

ADB_PREM   = open(Path.home() / ".config/databricks/adb_prem_host").read().strip()
PREM_TOKEN = open(Path.home() / ".config/databricks/adb_prem_token").read().strip()
WH_ID      = "ad52a9849c1c7ebf"
CATALOG    = "apex_dq_premium"
SCHEMA     = "pedido_sugerido"
DATA_DIR   = Path(__file__).parent.parent / "data"

H = {"Authorization": f"Bearer {PREM_TOKEN}", "Content-Type": "application/json"}

TABLES = {
    "clientes": {
        "file": "clientes.csv",
        "ddl": """(
            cliente_id STRING, nombre STRING, tipo STRING,
            pais STRING, region STRING, vendedor_id STRING,
            activo BOOLEAN, fecha_registro DATE
        )"""
    },
    "vendedores": {
        "file": "vendedores.csv",
        "ddl": "(vendedor_id STRING, nombre STRING, zona STRING)"
    },
    "skus": {
        "file": "skus.csv",
        "ddl": """(
            sku_id STRING, nombre STRING, categoria STRING,
            precio_caja DOUBLE, unidades_caja INT
        )"""
    },
    "historial_compras": {
        "file": "historial_compras.csv",
        "ddl": """(
            pedido_id STRING, cliente_id STRING, sku_id STRING,
            fecha DATE, semana INT, anio INT,
            cajas_pedidas INT, monto_total DOUBLE
        )"""
    },
    "stock_actual": {
        "file": "stock_actual.csv",
        "ddl": "(cliente_id STRING, sku_id STRING, stock_cajas INT, fecha_actualizacion DATE)"
    },
    "sugerencias_modelo": {
        "file": "sugerencias_modelo.csv",
        "ddl": """(
            cliente_id STRING, sku_id STRING, cajas_sugeridas INT,
            confianza_pct DOUBLE, tendencia_pct DOUBLE,
            razon_principal STRING, fecha_generacion DATE
        )"""
    },
    "pedidos_confirmados": {
        "file": None,
        "ddl": """(
            pedido_id STRING, cliente_id STRING, vendedor_id STRING,
            sku_id STRING, cajas_confirmadas INT,
            cajas_sugeridas INT, motivo_ajuste STRING,
            fecha_confirmacion TIMESTAMP, canal STRING
        )"""
    },
}

def run_sql(stmt, timeout=120):
    r = requests.post(f"{ADB_PREM}/api/2.0/sql/statements",
        headers=H, timeout=30,
        json={"statement": stmt, "warehouse_id": WH_ID,
              "wait_timeout": "30s", "on_wait_timeout": "CONTINUE",
              "format": "JSON_ARRAY"})
    r.raise_for_status()
    d = r.json(); sid = d["statement_id"]
    for _ in range(timeout // 3):
        state = d.get("status", {}).get("state", "UNKNOWN")
        if state in ("SUCCEEDED", "FAILED", "CANCELED"): break
        time.sleep(3)
        d = requests.get(f"{ADB_PREM}/api/2.0/sql/statements/{sid}", headers=H, timeout=15).json()
    if d.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {d.get('status',{}).get('error',{}).get('message','?')[:200]}")
    return d

def load_csv(table_name, csv_path, ddl):
    rows = list(csv.DictReader(open(csv_path)))
    cols = list(rows[0].keys())

    def q(v):
        if v is None or str(v).strip() in ("", "True", "False"):
            if str(v).strip() == "True":  return "TRUE"
            if str(v).strip() == "False": return "FALSE"
            return "NULL"
        return "'" + str(v).replace("'", "''") + "'"

    # Insertar en batches de 200
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        vals = ",\n  ".join("(" + ",".join(q(r[c]) for c in cols) + ")" for r in batch)
        run_sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{table_name} VALUES {vals}")

    return len(rows)

def main():
    print(f"🏗️  Creando schema {CATALOG}.{SCHEMA}...")
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print("   ✅ Schema listo\n")

    for table_name, cfg in TABLES.items():
        print(f"📦 Creando tabla {table_name}...")
        run_sql(f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{table_name} {cfg['ddl']} USING DELTA")

        if cfg["file"]:
            csv_path = DATA_DIR / cfg["file"]
            n = load_csv(table_name, csv_path, cfg["ddl"])
            print(f"   ✅ {n} filas cargadas")
        else:
            print(f"   ✅ Tabla vacía creada (se llenará en runtime)")

    print("\n🔍 Verificación:")
    for table_name in TABLES:
        r = run_sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table_name}")
        n = r["result"]["data_array"][0][0]
        print(f"   {table_name:25s} → {n} filas")

    print("\n✅ Setup Databricks completo!")

if __name__ == "__main__":
    main()
