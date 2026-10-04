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

import com.microsoft.spark.fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Leer la tabla de ventas
df_ventas = spark.table("ventas_enriquecidas")

# Obtener fecha mínima y máxima
rango_fechas = (
    df_ventas
    .select(
        F.min("fecha_hora").alias("fecha_min"),
        F.max("fecha_hora").alias("fecha_max")
    )
    .first()
)

fecha_min = rango_fechas["fecha_min"]
fecha_max = rango_fechas["fecha_max"]

print(f"Fecha mínima: {fecha_min}")
print(f"Fecha máxima: {fecha_max}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_fecha = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{fecha_min}'),
                to_date('{fecha_max}'),
                interval 1 day
            )
        ) AS fecha
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_fecha = (
    df_dim_fecha
    .withColumn("id_fecha", F.date_format("fecha", "yyyyMMdd").cast("int"))
    .withColumn("año", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
    .withColumn("mes_nombre_corto", F.date_format("fecha", "MMM"))
    .withColumn("trimestre", F.quarter("fecha"))
    .withColumn("nombre_trimestre", F.concat(F.lit("Q"), F.quarter("fecha")))
    .withColumn("dia", F.dayofmonth("fecha"))
    .withColumn("dia_semana", F.dayofweek("fecha"))
    .withColumn("nombre_dia_semana", F.date_format("fecha", "EEEE"))
    .withColumn("semana_año", F.weekofyear("fecha"))
    .withColumn("es_fin_de_semana", F.dayofweek("fecha").isin([1, 7]))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_dim_fecha.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_fecha.write \
    .mode("overwrite") \
    .synapsesql("WareHouseGold.dbo.dim_fecha")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meses = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre"
}

dias = {
    1: "Domingo",
    2: "Lunes",
    3: "Martes",
    4: "Miércoles",
    5: "Jueves",
    6: "Viernes",
    7: "Sábado"
}

mes_expr = F.create_map(
    *[x for k, v in meses.items() for x in (F.lit(k), F.lit(v))]
)

dia_expr = F.create_map(
    *[x for k, v in dias.items() for x in (F.lit(k), F.lit(v))]
)

df_dim_fecha = (
    df_dim_fecha
    .withColumn("id_fecha", F.date_format("fecha", "yyyyMMdd").cast("int"))
    .withColumn("año", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("nombre_mes", mes_expr[F.month("fecha")])
    .withColumn("trimestre", F.quarter("fecha"))
    .withColumn("nombre_trimestre", F.concat(F.lit("T"), F.quarter("fecha")))
    .withColumn("dia", F.dayofmonth("fecha"))
    .withColumn("dia_semana", F.dayofweek("fecha"))
    .withColumn("nombre_dia_semana", dia_expr[F.dayofweek("fecha")])
    .withColumn("semana_año", F.weekofyear("fecha"))
    .withColumn(
        "es_fin_de_semana",
        F.dayofweek("fecha").isin([1, 7])
    )
)
display(df_dim_fecha)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
