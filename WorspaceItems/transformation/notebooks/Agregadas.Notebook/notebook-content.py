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

df_fact_ventas = spark.read.synapsesql("WareHouseGold.gold.FactVentas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_fact_ventas.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_diarias = (
    df_fact_ventas
    .groupBy(
        "tienda_key",
        "fecha_key"
    )
    .agg(
        F.sum("venta_total").alias("venta_total"),
        F.sum("cantidad").alias("unidades_vendidas")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_diarias.printSchema()

display(
    df_ventas_diarias
    .orderBy("fecha_key", "tienda_key")
    .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_diarias.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.ventas_diarias_por_tienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real = (
    df_fact_ventas
    .groupBy(
        "tienda_key",
        "fecha_key"
    )
    .agg(
        F.sum("venta_total").alias("venta_total_dia")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_real.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_dim_fecha = spark.read.synapsesql("WareHouseGold.gold.DimFecha")
df_gold_dim_tienda = spark.read.synapsesql("WareHouseGold.gold.DimTienda")
df_presupuesto = spark.read.table("silver.presupuesto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real = (
    df_real.alias("r")
    .join(
        df_gold_dim_fecha.alias("f"),
        F.col("r.fecha_key") == F.col("f.fecha_key"),
        "left"
    )
    .select(
        F.col("r.tienda_key"),
        F.col("r.fecha_key"),
        F.col("r.venta_total_dia"),
        F.col("f.anio"),
        F.col("f.mes")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_real.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_mensual = (
    df_real
    .groupBy(
        "tienda_key",
        "anio",
        "mes"
    )
    .agg(
        F.sum("venta_total_dia").alias("venta_total_real")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_real_mensual.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_mensual = (
    df_real_mensual.alias("r")
    .join(
        df_gold_dim_tienda.alias("t"),
        F.col("r.tienda_key") == F.col("t.tienda_key"),
        "left"
    )
    .select(
        F.col("t.tienda_id"),
        F.col("r.anio"),
        F.col("r.mes"),
        F.col("r.venta_total_real")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_mensual = (
    df_real_mensual
    .withColumn(
        "anio_mes",
        F.to_date(
            F.concat(
                F.col("anio").cast("string"),
                F.lit("-"),
                F.lpad(
                    F.col("mes").cast("string"),
                    2,
                    "0"
                ),
                F.lit("-01")
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_real_mensual.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_vs_presupuesto = (
    df_real_mensual.alias("r")
    .join(
        df_presupuesto.alias("p"),
        (
            (F.col("r.tienda_id") == F.col("p.tienda_id")) &
            (F.col("r.anio_mes") == F.col("p.anio_mes"))
        ),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_real_vs_presupuesto.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_vs_presupuesto = (
    df_real_vs_presupuesto
    .withColumn(
        "variacion_pct",
        F.when(
            F.col("p.meta_venta").isNull() |
            (F.col("p.meta_venta") == 0),
            None
        )
        .otherwise(
            (
                F.col("r.venta_total_real") -
                F.col("p.meta_venta")
            )
            / F.col("p.meta_venta")
            * 100
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_vs_presupuesto = (
    df_real_vs_presupuesto
    .select(
        F.col("r.tienda_id").alias("tienda_id"),
        F.col("r.anio_mes").alias("anio_mes"),
        F.col("r.venta_total_real").alias("venta_total_real"),
        F.col("p.meta_venta").alias("meta_venta"),
        F.col("variacion_pct")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_vs_presupuesto.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_real_vs_presupuesto
    .orderBy(
        "anio_mes",
        "tienda_id"
    )
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_real_vs_presupuesto.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.gold.real_vs_presupuesto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
