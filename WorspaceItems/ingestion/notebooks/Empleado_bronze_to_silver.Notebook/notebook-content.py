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

from pyspark.sql.functions import col, to_date, trim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empleados = (
    spark.table("LakeHouseBronze.dbo.empleados")
    .withColumn("empleado_id", col("empleado_id").cast("int"))
    .withColumn("tienda_id", col("tienda_id").cast("int"))
    .withColumn("fecha_ingreso", to_date(col("fecha_ingreso"), "yyyy/MM/dd"))
    .withColumn("salario", col("salario").cast("decimal(18,2)"))
    .withColumn("documento_identidad", trim(col("documento_identidad")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_empleados.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columnas = [
    "empleado_id",
    "nombre_completo",
    "tienda_id",
    "puesto",
    "fecha_ingreso",
    "salario",
    "documento_identidad"
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

df_empleados_validos = df_empleados.filter(condicion_valido)
df_rechazados = df_empleados.filter(f"NOT ({condicion_valido})")
display(df_empleados_validos.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Estructura requerida para la tabla

# CELL ********************

df_empleados.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType, DateType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

empleado_schema = StructType(fields= [
    StructField("empleado_id", IntegerType(), False),
    StructField("nombre_completo", StringType(), False),
    StructField("tienda_id", IntegerType(), False),
    StructField("puesto", StringType(), False),
    StructField("fecha_ingreso", DateType(), False),
    StructField("salario", DecimalType(18,2), False),
    StructField("documento_identidad", StringType(), False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empleados_final = spark.createDataFrame(
    df_empleados_validos.rdd,
    schema=empleado_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_empleados_final.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empleados_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empleados_final.write.format("delta").mode("overwrite").saveAsTable("dim_empleado")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_empleados_final.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM dim_empleado
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
