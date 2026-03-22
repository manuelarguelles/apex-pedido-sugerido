"""
init_sqlite.py — Carga los CSVs en SQLite local para el demo.
Mismos datos que Databricks, sin necesidad de conexión.
"""
import sqlite3, csv, json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH  = Path(__file__).parent / "apex_demo.db"

SCHEMA = {
    "clientes": "cliente_id TEXT, nombre TEXT, tipo TEXT, pais TEXT, region TEXT, vendedor_id TEXT, activo TEXT, fecha_registro TEXT",
    "vendedores": "vendedor_id TEXT, nombre TEXT, zona TEXT",
    "skus": "sku_id TEXT, nombre TEXT, categoria TEXT, precio_caja REAL, unidades_caja INTEGER",
    "historial_compras": "pedido_id TEXT, cliente_id TEXT, sku_id TEXT, fecha TEXT, semana INTEGER, anio INTEGER, cajas_pedidas INTEGER, monto_total REAL",
    "stock_actual": "cliente_id TEXT, sku_id TEXT, stock_cajas INTEGER, fecha_actualizacion TEXT",
    "sugerencias_modelo": "cliente_id TEXT, sku_id TEXT, cajas_sugeridas INTEGER, confianza_pct REAL, tendencia_pct REAL, razon_principal TEXT, fecha_generacion TEXT",
    "pedidos_confirmados": "pedido_id TEXT, cliente_id TEXT, vendedor_id TEXT, sku_id TEXT, cajas_confirmadas INTEGER, cajas_sugeridas INTEGER, motivo_ajuste TEXT, fecha_confirmacion TEXT, canal TEXT",
}

def init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    for table, cols in SCHEMA.items():
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} ({cols})")

        csv_file = DATA_DIR / f"{table}.csv"
        if csv_file.exists():
            rows = list(csv.reader(open(csv_file)))
            headers = rows[0]
            data = rows[1:]
            placeholders = ",".join(["?" for _ in headers])
            cur.executemany(f"INSERT INTO {table} VALUES ({placeholders})", data)
            print(f"✅ {table}: {len(data)} filas")
        else:
            print(f"✅ {table}: tabla vacía")

    con.commit()
    con.close()
    print(f"\n✅ SQLite lista: {DB_PATH}")

if __name__ == "__main__":
    init()
