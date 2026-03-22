"""
generate_data.py — Genera datos sintéticos realistas para el demo Apex Pedido Sugerido.
Contexto: Grupo Mariposa, distribuidor Pepsi en Centroamérica.
"""
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path
import random

random.seed(42)
np.random.seed(42)
OUT = Path(__file__).parent

# ── SKUs Pepsi Centroamérica ──────────────────────────────────────────────
SKUS = [
    {"sku_id": "SKU001", "nombre": "Pepsi Cola 600ml",      "categoria": "Cola",     "precio_caja": 18.50, "unidades_caja": 24},
    {"sku_id": "SKU002", "nombre": "Pepsi Cola 1.5L",       "categoria": "Cola",     "precio_caja": 22.00, "unidades_caja": 12},
    {"sku_id": "SKU003", "nombre": "Pepsi Cola 2L",         "categoria": "Cola",     "precio_caja": 24.50, "unidades_caja": 8},
    {"sku_id": "SKU004", "nombre": "Pepsi Cola 355ml Lata", "categoria": "Cola",     "precio_caja": 20.00, "unidades_caja": 24},
    {"sku_id": "SKU005", "nombre": "7UP 600ml",             "categoria": "Lima-Limón","precio_caja": 17.50, "unidades_caja": 24},
    {"sku_id": "SKU006", "nombre": "7UP 1.5L",              "categoria": "Lima-Limón","precio_caja": 21.00, "unidades_caja": 12},
    {"sku_id": "SKU007", "nombre": "Mirinda Naranja 600ml", "categoria": "Sabores",  "precio_caja": 17.00, "unidades_caja": 24},
    {"sku_id": "SKU008", "nombre": "Mirinda Uva 600ml",     "categoria": "Sabores",  "precio_caja": 17.00, "unidades_caja": 24},
    {"sku_id": "SKU009", "nombre": "Agua H2Oh! 500ml",      "categoria": "Agua",     "precio_caja": 12.00, "unidades_caja": 24},
    {"sku_id": "SKU010", "nombre": "Gatorade Limón 500ml",  "categoria": "Isotónica","precio_caja": 28.00, "unidades_caja": 12},
]

# ── Clientes por tipo y país ──────────────────────────────────────────────
TIPOS_CLIENTE = {
    "tienda_barrio":   {"peso": 0.50, "ticket_base": 2.5,  "skus_top": ["SKU001","SKU005","SKU009","SKU007"]},
    "supermercado":    {"peso": 0.20, "ticket_base": 8.0,  "skus_top": ["SKU001","SKU002","SKU003","SKU010"]},
    "restaurante":     {"peso": 0.20, "ticket_base": 4.0,  "skus_top": ["SKU001","SKU004","SKU005","SKU009"]},
    "distribuidor":    {"peso": 0.10, "ticket_base": 15.0, "skus_top": ["SKU001","SKU002","SKU005","SKU009"]},
}

PAISES = ["Guatemala", "Honduras", "El Salvador", "Nicaragua", "Costa Rica"]
REGIONES = {
    "Guatemala":   ["Ciudad de Guatemala", "Quetzaltenango", "Escuintla"],
    "Honduras":    ["Tegucigalpa", "San Pedro Sula", "La Ceiba"],
    "El Salvador": ["San Salvador", "Santa Ana", "San Miguel"],
    "Nicaragua":   ["Managua", "León", "Granada"],
    "Costa Rica":  ["San José", "Alajuela", "Heredia"],
}

VENDEDORES = [
    {"vendedor_id": f"VEN{i:02d}", "nombre": nombre, "zona": zona}
    for i, (nombre, zona) in enumerate([
        ("Carlos Reyes",    "Guatemala Norte"),
        ("Ana Flores",      "Guatemala Sur"),
        ("Luis Martínez",   "Honduras Central"),
        ("María García",    "Honduras Norte"),
        ("José Hernández",  "El Salvador"),
        ("Laura Pérez",     "Nicaragua"),
        ("Roberto Díaz",    "Costa Rica"),
    ], 1)
]

NOMBRES_TIENDAS = [
    "La Esperanza", "El Progreso", "San José", "La Unión", "El Triunfo",
    "La Victoria", "Nueva Vida", "El Paraíso", "La Bendición", "El Éxito",
    "Mi Tienda", "El Buen Precio", "La Económica", "El Ahorro", "La Popular",
]
APELLIDOS = ["Rodríguez","González","López","Pérez","Martínez","García","Hernández","Díaz","Torres","Morales"]

def gen_clientes(n=50):
    clientes = []
    tipo_list = list(TIPOS_CLIENTE.keys())
    pesos = [TIPOS_CLIENTE[t]["peso"] for t in tipo_list]
    vendedor_ids = [v["vendedor_id"] for v in VENDEDORES]

    for i in range(1, n+1):
        pais   = random.choice(PAISES)
        region = random.choice(REGIONES[pais])
        tipo   = random.choices(tipo_list, weights=pesos)[0]

        if tipo == "tienda_barrio":
            nombre = f"Tienda {random.choice(NOMBRES_TIENDAS)} {random.choice(APELLIDOS)}"
        elif tipo == "supermercado":
            nombre = f"Super {random.choice(NOMBRES_TIENDAS)}"
        elif tipo == "restaurante":
            nombre = f"Restaurante {random.choice(NOMBRES_TIENDAS)}"
        else:
            nombre = f"Distribuidora {random.choice(APELLIDOS)} & Hnos."

        clientes.append({
            "cliente_id":   f"CLI{i:03d}",
            "nombre":       nombre,
            "tipo":         tipo,
            "pais":         pais,
            "region":       region,
            "vendedor_id":  random.choice(vendedor_ids),
            "activo":       True,
            "fecha_registro": (datetime(2023,1,1) + timedelta(days=random.randint(0,365))).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(clientes)

def gen_historial(clientes_df, semanas=26):
    """6 meses de historial semanal por cliente x SKU."""
    records = []
    hoy = datetime.now()
    sku_ids = [s["sku_id"] for s in SKUS]
    sku_map  = {s["sku_id"]: s for s in SKUS}

    for _, cli in clientes_df.iterrows():
        tipo_cfg = TIPOS_CLIENTE[cli["tipo"]]
        skus_top = tipo_cfg["skus_top"]
        ticket   = tipo_cfg["ticket_base"]

        # tendencia aleatoria por cliente (-10% a +30%)
        tendencia = 1 + np.random.uniform(-0.10, 0.30)

        for sem in range(semanas, 0, -1):
            fecha = hoy - timedelta(weeks=sem)
            # no todos los clientes compran todas las semanas
            if random.random() < 0.75:
                # SKUs activos esta semana (top + 1-2 aleatorios)
                skus_semana = skus_top + random.sample([s for s in sku_ids if s not in skus_top], k=2)
                for sku_id in skus_semana:
                    base_cajas = ticket if sku_id in skus_top else ticket * 0.3
                    # añadir tendencia y ruido
                    factor_tend = 1 + (tendencia - 1) * ((semanas - sem) / semanas)
                    cajas = max(1, int(base_cajas * factor_tend * np.random.uniform(0.7, 1.3)))
                    precio = sku_map[sku_id]["precio_caja"]
                    records.append({
                        "pedido_id":   f"PED{len(records)+1:06d}",
                        "cliente_id":  cli["cliente_id"],
                        "sku_id":      sku_id,
                        "fecha":       fecha.strftime("%Y-%m-%d"),
                        "semana":      fecha.isocalendar()[1],
                        "año":         fecha.year,
                        "cajas_pedidas": cajas,
                        "monto_total": round(cajas * precio, 2),
                    })
    return pd.DataFrame(records)

def gen_stock_actual(clientes_df):
    """Stock actual en cada punto de venta."""
    records = []
    sku_ids = [s["sku_id"] for s in SKUS]
    for _, cli in clientes_df.iterrows():
        for sku_id in sku_ids:
            # stock bajo en ~30% de los casos (oportunidad)
            stock = random.choices(
                [0, random.randint(1,3), random.randint(3,8), random.randint(8,20)],
                weights=[0.10, 0.20, 0.40, 0.30]
            )[0]
            records.append({
                "cliente_id": cli["cliente_id"],
                "sku_id":     sku_id,
                "stock_cajas": stock,
                "fecha_actualizacion": datetime.now().strftime("%Y-%m-%d"),
            })
    return pd.DataFrame(records)

def gen_sugerencias(clientes_df, historial_df):
    """
    Modelo simple de sugerencia: promedio últimas 4 semanas * factor_tendencia.
    En el Sprint 1 esto se reemplaza por sklearn real.
    """
    hoy = datetime.now()
    hace_4sem = hoy - timedelta(weeks=4)
    hace_8sem = hoy - timedelta(weeks=8)

    historial_df["fecha_dt"] = pd.to_datetime(historial_df["fecha"])
    reciente = historial_df[historial_df["fecha_dt"] >= hace_4sem]
    anterior = historial_df[(historial_df["fecha_dt"] >= hace_8sem) & (historial_df["fecha_dt"] < hace_4sem)]

    avg_rec = reciente.groupby(["cliente_id","sku_id"])["cajas_pedidas"].mean().reset_index().rename(columns={"cajas_pedidas":"avg_rec"})
    avg_ant = anterior.groupby(["cliente_id","sku_id"])["cajas_pedidas"].mean().reset_index().rename(columns={"cajas_pedidas":"avg_ant"})

    sug = avg_rec.merge(avg_ant, on=["cliente_id","sku_id"], how="left")
    sug["avg_ant"] = sug["avg_ant"].fillna(sug["avg_rec"])
    sug["tendencia_pct"] = ((sug["avg_rec"] - sug["avg_ant"]) / sug["avg_ant"].replace(0,1) * 100).round(1)

    # Sugerencia = promedio reciente * factor tendencia, redondeado
    sug["factor"] = 1 + sug["tendencia_pct"].clip(-20, 40) / 100
    sug["cajas_sugeridas"] = (sug["avg_rec"] * sug["factor"]).clip(lower=1).round().astype(int)
    sug["confianza_pct"] = (70 + np.random.uniform(0, 25, len(sug))).clip(0,99).round(1)

    # Razón principal
    def razon(row):
        if row["tendencia_pct"] >= 15: return f"Tendencia de crecimiento +{row['tendencia_pct']}% en 4 semanas"
        if row["tendencia_pct"] <= -10: return f"Tendencia a la baja {row['tendencia_pct']}% — ajuste preventivo"
        return "Demanda estable — reposición basada en promedio histórico"

    sug["razon_principal"] = sug.apply(razon, axis=1)
    sug["fecha_generacion"] = datetime.now().strftime("%Y-%m-%d")

    return sug[["cliente_id","sku_id","cajas_sugeridas","confianza_pct","tendencia_pct","razon_principal","fecha_generacion"]]

if __name__ == "__main__":
    print("Generando datos sintéticos Apex Pedido Sugerido...")

    clientes  = gen_clientes(50)
    vendedores = pd.DataFrame(VENDEDORES)
    skus      = pd.DataFrame(SKUS)
    historial = gen_historial(clientes)
    stock     = gen_stock_actual(clientes)
    sugerencias = gen_sugerencias(clientes, historial)

    clientes.to_csv(OUT/"clientes.csv", index=False)
    vendedores.to_csv(OUT/"vendedores.csv", index=False)
    skus.to_csv(OUT/"skus.csv", index=False)
    historial.to_csv(OUT/"historial_compras.csv", index=False)
    stock.to_csv(OUT/"stock_actual.csv", index=False)
    sugerencias.to_csv(OUT/"sugerencias_modelo.csv", index=False)

    print(f"✅ clientes:      {len(clientes)} filas")
    print(f"✅ vendedores:    {len(vendedores)} filas")
    print(f"✅ skus:          {len(skus)} filas")
    print(f"✅ historial:     {len(historial)} filas")
    print(f"✅ stock_actual:  {len(stock)} filas")
    print(f"✅ sugerencias:   {len(sugerencias)} filas")
    print("\nDatos guardados en data/")
