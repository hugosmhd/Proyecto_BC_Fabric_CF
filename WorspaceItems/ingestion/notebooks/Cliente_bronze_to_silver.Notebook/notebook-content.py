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

from pyspark.sql.functions import col, to_date, lit, coalesce

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clientes = (
    spark.table("LakeHouseBronze.dbo.clientes")
    .withColumn("cliente_id", col("cliente_id").cast("int"))
    .withColumn("fecha_registro", to_date(col("fecha_registro"), "yyyy/MM/dd"))
    .withColumn("tier_lealtad", coalesce(col("tier_lealtad"), lit("Sin Tier")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_clientes.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columnas = [
    "cliente_id",
    "nombre_completo",
    "fecha_registro",
    "tier_lealtad"
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

df_clientes_validos = df_clientes.filter(condicion_valido)
df_rechazados = df_clientes.filter(f"NOT ({condicion_valido})")
display(df_clientes_validos.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cliente_schema = StructType(fields= [
    StructField("cliente_id", IntegerType(), False),
    StructField("nombre_completo", StringType(), False),
    StructField("fecha_registro", DateType(), False),
    StructField("tier_lealtad", StringType(), False),
    StructField("ciudad", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clientes_final = spark.createDataFrame(
    df_clientes_validos.rdd,
    schema=cliente_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clientes_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clientes_final.write.format("delta").mode("overwrite").saveAsTable("dim_cliente")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM dim_cliente
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
