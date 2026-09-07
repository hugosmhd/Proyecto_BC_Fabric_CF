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

# MARKDOWN ********************

# ### Cargar tabla y transformaciones

# CELL ********************

from pyspark.sql.functions import col, to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1. Se cambia tipo tienda_id a int
# 2. Se cambia tipo fecha_apertura a date

# CELL ********************

df_tienda = (
    spark.table("LakeHouseBronze.bronze.tiendas")
    .withColumn("tienda_id", col("tienda_id").cast("int"))
    .withColumn("fecha_apertura", to_date(col("fecha_apertura"), "yyyy/MM/dd"))
    #.withColumn("fecha_apertura", col("fecha_apertura").cast("date"))
)

display(df_tienda.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tienda.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Ver que filas no cumplen con todos los campos

# CELL ********************

columnas = [
    "tienda_id",
    "nombre_tienda",
    "ciudad",
    "pais",
    "region",
    "formato_tienda",
    "fecha_apertura"
]

condicion_null = " OR ".join([f"`{c}` IS NULL" for c in columnas])

df_invalidos = df_tienda.filter(condicion_null)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_invalidos.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_invalidos.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Estructura requerida para la tabla

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tienda_schema = StructType(fields= [     
    StructField("tienda_id", IntegerType(), False),     
    StructField("nombre_tienda", StringType(), False),     
    StructField("ciudad", StringType(), False),     
    StructField("pais", StringType(), False),     
    StructField("region", StringType(), False),     
    StructField("formato_tienda", StringType(), False),     
    StructField("fecha_apertura", DateType(), False) 
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# columnas = [field.name for field in tienda_schema]

condicion_valido = " AND ".join(
    [f"`{c}` IS NOT NULL" for c in columnas]
)

df_tienda_validos = df_tienda.filter(condicion_valido)
df_rechazados = df_tienda.filter(f"NOT ({condicion_valido})")
display(df_tienda_validos.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tienda_final = spark.createDataFrame(
    df_tienda_validos.rdd,
    schema=tienda_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_tienda_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Escribir en LakeHouseSilver

# CELL ********************

df_tienda_final.write.format("delta").mode("overwrite").saveAsTable("silver.dim_tienda")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver.dim_tienda
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
