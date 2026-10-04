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
# META         },
# META         {
# META           "id": "ab3dc20a-eb42-4f45-9ee6-4a8e726e769b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_presupuesto = (spark.table("LakeHouseBronze.bronze.presupuesto_metas")
                    .withColumn("tienda_id", col("tienda_id").cast("int"))
                    .withColumn("anio_mes", to_date(col("anio_mes"), "yyyy/MM"))
                    .withColumn("meta_venta", col("meta_venta").cast("decimal(18,2)"))
                    .withColumn("meta_margen", col("meta_margen").cast("decimal(18,2)")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_presupuesto.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columnas = [
    "tienda_id",
    "anio_mes",
    "meta_venta",
    "meta_margen"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

condicion_valido = " AND ".join(
    [f"`{c}` IS NOT NULL" for c in columnas]
)

df_presupuesto_validos = df_presupuesto.filter(condicion_valido)
df_rechazados = df_presupuesto.filter(f"NOT ({condicion_valido})")
display(df_presupuesto_validos.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType, StringType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

presupuesto_schema = StructType(fields= [
    StructField("tienda_id", IntegerType(), False),
    StructField("anio_mes", DateType(), False),
    StructField("meta_venta", DecimalType(18,2), False),
    StructField("meta_margen", DecimalType(18,2), False),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_presupuesto_final = spark.createDataFrame(
    df_presupuesto_validos.rdd,
    schema=presupuesto_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_presupuesto_final.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_presupuesto_final.write.format("delta").mode("overwrite").saveAsTable("silver.presupuesto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver.presupuesto
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
