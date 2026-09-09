# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "335a784e-dde1-45b8-aeb0-531cf1adc6ec",
# META       "default_lakehouse_name": "LakeHouseSilver",
# META       "default_lakehouse_workspace_id": "362a3821-c2d0-49bd-af6a-7956d26f207f",
# META       "known_lakehouses": [
# META         {
# META           "id": "335a784e-dde1-45b8-aeb0-531cf1adc6ec"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# ============================================================
# 1. Leer Silver
# ============================================================

ventas = spark.table("dbo.ventas_enriquecidas")

# Dimensiones
dim_tienda = spark.table("dbo.dim_tienda")
dim_producto = spark.table("dbo.dim_producto")
dim_empleado = spark.table("dbo.dim_empleado")
dim_cliente = spark.table("dbo.dim_cliente")

display(ventas.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 2. Cruzar con dim_tienda
#    tienda_id -> tienda_id
# ============================================================

ventas = (
    ventas.alias("v")
    .join(
        dim_tienda.select(
            "tienda_id",
            "tienda_id"
        ).alias("t"),
        F.col("v.tienda_id") == F.col("t.tienda_id"),
        "left"
    )
    .select(
        "v.*"
        #F.col("t.tienda_id")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 3. Cruzar con dim_producto
#    sku -> sku + costo_unitario
# ============================================================

ventas = (
    ventas.alias("v")
    .join(
        dim_producto.select(
            "sku",
            "sku",
            "costo_unitario"
        ).alias("p"),
        F.col("v.sku") == F.col("p.sku"),
        "left"
    )
    .select(
        "v.*",
        #F.col("p.sku"),
        F.col("p.costo_unitario")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 4. Cruzar con dim_empleado
#    empleado_id -> empleado_id
# ============================================================

ventas = (
    ventas.alias("v")
    .join(
        dim_empleado.select(
            "empleado_id",
            "empleado_id"
        ).alias("e"),
        F.col("v.empleado_id") == F.col("e.empleado_id"),
        "left"
    )
    .select(
        "v.*"
        #F.col("e.empleado_id")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 5. Cruzar con dim_cliente
#    cliente_id vacío -> cliente_id = -1
# ============================================================

ventas = (
    ventas.alias("v")
    .join(
        dim_cliente.select(
            "cliente_id",
            "cliente_id"
        ).alias("c"),
        F.col("v.cliente_id") == F.col("c.cliente_id"),
        "left"
    )
    .select(
        "v.*"
        #F.col("c.cliente_id")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ventas = ventas.select(
    "transaccion_id",
    "fecha_id",
    "tienda_id",
    "producto_id",
    "empleado_id",
    "cliente_id"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Si cliente_id está vacío, usar -1.
ventas = ventas.withColumn(
    "cliente_id",
    F.when(
        F.col("cliente_id").isNull() |
        (F.trim(F.col("cliente_id").cast("string")) == ""),
        F.lit(-1)
    ).otherwise(F.col("cliente_id"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(ventas.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 6. Marcar ventas para revisión
#    No se descartan las ventas.
# ============================================================

ventas_revision = (
    ventas
    .withColumn(
        "requiere_revision",
        F.col("tienda_id").isNull() |
        F.col("sku").isNull() |
        F.col("empleado_id").isNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Opcional: comprobar cuántas ventas requieren revisión
ventas_revision.groupBy("requiere_revision").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 7. Calcular venta_total
# ============================================================

ventas_revision = ventas_revision.withColumn(
    "venta_total",
    F.col("cantidad") *
    F.col("precio_unitario") *
    (F.lit(1) - F.col("descuento_pct") / F.lit(100))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 8. Calcular costo_total
#    costo_unitario viene de dim_producto
# ============================================================

ventas_revision = ventas_revision.withColumn(
    "costo_total",
    F.col("cantidad") * F.col("costo_unitario")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 9. Seleccionar ÚNICAMENTE los campos finales
# ============================================================

fact_ventas = ventas_revision.select(
    "transaccion_id",
    #"fecha_id",
    "tienda_id",
    "sku",
    "empleado_id",
    "cliente_id",
    "cantidad",
    "precio_unitario",
    "descuento_pct",
    "venta_total",
    "costo_total"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ventas = spark.table("dbo.ventas_enriquecidas")
tiendas = spark.table("dbo.dim_tienda")
productos = spark.table("dbo.dim_producto")
empleado = spark.table("dbo.dim_empleado")
cliente = spark.table("dbo.dim_cliente")

ventas_enriquecidas = (
    ventas.alias("v")
    .join(
        tiendas.alias("t"),
        F.col("v.tienda_id") == F.col("t.tienda_id"),
        "left"
    )
    .join(
        productos.alias("p"),
        F.col("v.sku") == F.col("p.sku"),
        "left"
    )
    .join(
        empleado.alias("e"),
        F.col("v.empleado_id") == F.col("e.empleado_id"),
        "left"
    )
    .join(
        cliente.alias("c"),
        F.coalesce(F.col("v.cliente_id"), F.lit(-1)) == F.col("c.cliente_id"),
        "left"
    )
)

display(ventas_enriquecidas.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
