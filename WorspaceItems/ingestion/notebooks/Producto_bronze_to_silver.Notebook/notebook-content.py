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

from pyspark.sql.functions import col, trim, initcap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_producto = (
    spark.table("LakeHouseBronze.dbo.productos")
    .withColumn("costo_unitario", col("costo_unitario").cast("decimal(18,2)"))
    .withColumn("nombre_producto", trim(col("nombre_producto")))
    .withColumn("categoria", initcap(trim(col("categoria"))))
)

display(df_producto.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_producto.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Ver objetos que no cumplen con las columnas requeridas

# CELL ********************

columnas = [
    "sku",
    "nombre_producto",
    "categoria",
    "subcategoria",
    #"marca",
    "costo_unitario"
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

df_producto_validos = df_producto.filter(condicion_valido)
df_rechazados = df_producto.filter(f"NOT ({condicion_valido})")
display(df_rechazados)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Estructura requerida para la tabla

# CELL ********************

from pyspark.sql.types import StructType, StructField, DecimalType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

producto_schema = StructType(fields= [     
    StructField("sku", StringType(), False),     
    StructField("nombre_producto", StringType(), False),     
    StructField("categoria", StringType(), False),     
    StructField("subcategoria", StringType(), False),     
    StructField("marca", StringType(), True),     
    StructField("costo_unitario", DecimalType(18,2), False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_producto_final = spark.createDataFrame(
    df_producto_validos.rdd,
    schema=producto_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_producto_final.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_producto_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_producto_final.write.format("delta").mode("overwrite").saveAsTable("dim_producto")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_producto_final.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM dim_producto
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
