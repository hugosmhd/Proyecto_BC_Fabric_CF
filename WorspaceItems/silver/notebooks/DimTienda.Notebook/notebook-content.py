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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "2fcb082c-92ce-8bb5-4192-2345e17d43f5",
# META       "known_warehouses": [
# META         {
# META           "id": "2fcb082c-92ce-8bb5-4192-2345e17d43f5",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import com.microsoft.spark.fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_tienda = spark.read.table("silver.dim_tienda")
df_dim_producto = spark.read.table("silver.dim_producto")
df_dim_empleado = spark.read.table("silver.dim_empleado")
df_dim_cliente = spark.read.table("silver.dim_cliente")
df_presupuesto = spark.read.table("silver.presupuesto")
df_ventas = spark.read.table("silver.ventas_enriquecidas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_tienda.printSchema()
df_dim_producto.printSchema()
df_dim_empleado.printSchema()
df_dim_cliente.printSchema()
df_presupuesto.printSchema()
df_ventas.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_tienda = Window.orderBy("tienda_id")

df_gold_dim_tienda = (
    df_dim_tienda
    .withColumn(
        "tienda_key",
        F.row_number().over(window_tienda)
    )
    .select(
        "tienda_key",
        "tienda_id",
        "nombre_tienda",
        "ciudad",
        "pais",
        "region",
        "formato_tienda",
        "fecha_apertura"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_dim_tienda.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimProducto

# CELL ********************

window_producto = Window.orderBy("sku")

df_gold_dim_producto = (
    df_dim_producto
    .withColumn(
        "producto_key",
        F.row_number().over(window_producto)
    )
    .select(
        "producto_key",
        "sku",
        "nombre_producto",
        "categoria",
        "subcategoria",
        "marca",
        "costo_unitario"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_dim_producto.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimEmpleado

# CELL ********************

window_empleado = Window.orderBy("empleado_id")

df_gold_dim_empleado = (
    df_dim_empleado
    .withColumn(
        "empleado_key",
        F.row_number().over(window_empleado)
    )
    .select(
        "empleado_key",
        "empleado_id",
        "nombre_completo",
        "tienda_id",
        "puesto",
        "fecha_ingreso",
        "salario",
        "documento_identidad"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_dim_empleado.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimCliente

# CELL ********************

window_cliente = Window.orderBy("cliente_id")

df_gold_dim_cliente = (
    df_dim_cliente
    .withColumn(
        "cliente_key",
        F.row_number().over(window_cliente)
    )
    .select(
        "cliente_key",
        "cliente_id",
        "nombre_completo",
        "fecha_registro",
        "tier_lealtad",
        "ciudad"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_dim_cliente.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cliente_no_identificado = spark.createDataFrame(
    [
        (
            -1,
            None,
            "Cliente No Identificado",
            None,
            "Sin Tier",
            None
        )
    ],
    schema=df_gold_dim_cliente.schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_cliente = (
    cliente_no_identificado
    .unionByName(df_gold_dim_cliente)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_dim_cliente.orderBy("cliente_key").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DimFecha

# CELL ********************

rango_fechas = df_ventas.select(
    F.min("fecha").alias("fecha_min"),
    F.max("fecha").alias("fecha_max")
).collect()[0]

fecha_min = rango_fechas["fecha_min"]
fecha_max = rango_fechas["fecha_max"]

print("Fecha mínima:", fecha_min)
print("Fecha máxima:", fecha_max)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_fecha = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(fecha_min),
                F.lit(fecha_max),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("fecha")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_fecha = (
    df_gold_dim_fecha
    .withColumn(
        "fecha_key",
        F.date_format("fecha", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "anio",
        F.year("fecha")
    )
    .withColumn(
        "mes",
        F.month("fecha")
    )
    .withColumn(
        "trimestre",
        F.quarter("fecha")
    )
    .withColumn(
        "nombre_mes",
        F.date_format("fecha", "MMMM")
    )
    .withColumn(
        "dia_semana",
        F.date_format("fecha", "EEEE")
    )
    .select(
        "fecha_key",
        "fecha",
        "anio",
        "mes",
        "trimestre",
        "nombre_mes",
        "dia_semana"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_gold_dim_fecha.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### FactVentas

# CELL ********************

df_fact_ventas = (
    df_ventas.alias("v")

    # Tienda
    .join(
        df_gold_dim_tienda.alias("t"),
        F.col("v.tienda_id") == F.col("t.tienda_id"),
        "left"
    )

    # Producto
    .join(
        df_gold_dim_producto.alias("p"),
        F.col("v.sku") == F.col("p.sku"),
        "left"
    )

    # Empleado
    .join(
        df_gold_dim_empleado.alias("e"),
        F.col("v.empleado_id") == F.col("e.empleado_id"),
        "left"
    )

    # Cliente
    .join(
        df_gold_dim_cliente.alias("c"),
        F.col("v.cliente_id") == F.col("c.cliente_id"),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_ventas = (
    df_fact_ventas
    .withColumn(
        "cliente_key",
        F.coalesce(
            F.col("c.cliente_key"),
            F.lit(-1)
        )
    )
    .withColumn(
        "costo_total",
        F.col("v.cantidad") * F.col("p.costo_unitario")
    )
    .select(
        F.col("v.transaccion_id").alias("transaccion_id"),

        F.date_format(
            F.col("v.fecha"),
            "yyyyMMdd"
        ).cast("int").alias("fecha_key"),

        F.col("t.tienda_key").alias("tienda_key"),
        F.col("p.producto_key").alias("producto_key"),
        F.col("e.empleado_key").alias("empleado_key"),
        F.col("cliente_key"),

        F.col("v.cantidad").alias("cantidad"),
        F.col("v.precio_unitario").alias("precio_unitario"),
        F.col("v.descuento_pct").alias("descuento_pct"),
        F.col("v.venta_total").alias("venta_total"),
        F.col("costo_total")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_fact_ventas.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Tiendas sin correspondencia:")
print(
    df_fact_ventas
    .filter(F.col("tienda_key").isNull())
    .count()
)

print("Productos sin correspondencia:")
print(
    df_fact_ventas
    .filter(F.col("producto_key").isNull())
    .count()
)

print("Empleados sin correspondencia:")
print(
    df_fact_ventas
    .filter(F.col("empleado_key").isNull())
    .count()
)

print("Clientes no identificados:")
print(
    df_fact_ventas
    .filter(F.col("cliente_key") == -1)
    .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_validacion = (
    df_fact_ventas
    .withColumn(
        "venta_total_calculada",
        F.col("cantidad")
        * F.col("precio_unitario")
        * (
            1 - F.col("descuento_pct") / F.lit(100)
        )
    )
    .withColumn(
        "diferencia",
        F.abs(
            F.col("venta_total") -
            F.col("venta_total_calculada")
        )
    )
)

display(
    df_validacion
    .filter(F.col("diferencia") > 0.01)
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Escritura en Gold

# CELL ********************

df_gold_dim_tienda.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.DimTienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_producto.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.DimProducto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_empleado.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.DimEmpleado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_cliente.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.DimCliente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_fecha.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.DimFecha")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_ventas.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.FactVentas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Proceso inicial

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dim_tienda = spark.table("silver.dim_tienda")

window_tienda = Window.orderBy("tienda_id")

dim_tienda_gold = (
    dim_tienda
    .withColumn(
        "tienda_key",
        F.row_number().over(window_tienda)
    )
    .select(
        "tienda_key",
        "tienda_id",
        "nombre_tienda",
        "ciudad",
        "pais",
        "region",
        "formato_tienda",
        "fecha_apertura"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dim_tienda_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
