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

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

#df_bronze = spark.read.table("LakeHouseBronze.dbo.tiendas")
df = spark.read.format("delta").load("abfss://Proyecto_CF_DEV@onelake.dfs.fabric.microsoft.com/LakeHouseBronze.Lakehouse/Tables/dbo/tiendas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.limit(10))

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

df = spark.sql("SELECT * FROM LakeHouseBronze.dbo.tiendas")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_bronze = (
    spark.table("LakeHouseBronze.dbo.tiendas")
    .withColumn("tienda_id", col("tienda_id").cast("int"))
    .withColumn("fecha_apertura", col("fecha_apertura").cast("date"))
)

display(df_bronze.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.createDataFrame(
    df_bronze.rdd,
    schema=tienda_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
