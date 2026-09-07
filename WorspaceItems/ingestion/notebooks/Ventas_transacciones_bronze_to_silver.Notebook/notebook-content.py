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

from pyspark.sql.functions import col, coalesce, initcap, lit, to_date, to_timestamp, trim, regexp_replace, round

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_transacciones = (
    spark.table("LakeHouseBronze.bronze.ventas_transacciones")
    .withColumn("fecha", to_date(col("fecha"), "yyyy/MM/dd"))
    .withColumn("hora", to_timestamp(col("hora"), "HH:mm:ss"))
    .withColumn("tienda_id", col("tienda_id").cast("int"))
    .withColumn("empleado_id", col("empleado_id").cast("int"))
    .withColumn("cliente_id", col("cliente_id").cast("int"))
    .withColumn("cantidad", col("cantidad").cast("int"))
    .withColumn("precio_unitario", regexp_replace(trim(col("precio_unitario")), ",", ".")
        .cast("decimal(18,2)"))
    .withColumn("descuento_pct", coalesce(col("descuento_pct").cast("int"), lit(0)))
    .withColumn("medio_pago", initcap(trim(col("medio_pago"))))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_trans_with_total = df_ventas_transacciones \
                            .withColumn("venta_total", round(
                        col("cantidad")*col("precio_unitario")
                        *(1-(col("descuento_pct")/100)),2).cast("decimal(18,2)"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ventas_trans_with_total.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columnas = [
    "transaccion_id",
    "fecha",
    "hora",
    "tienda_id",
    "empleado_id",
    #"cliente_id",
    "sku",
    "cantidad",
    "precio_unitario",
    #"descuento_pct",
    "medio_pago",
    "venta_total"
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

df_ventas_validos = df_ventas_trans_with_total.filter(condicion_valido)
df_rechazados = df_ventas_trans_with_total.filter(f"NOT ({condicion_valido})")
display(df_ventas_validos.limit(10))

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

ventas_schema = StructType(fields= [
    StructField("transaccion_id", StringType(), False),
    StructField("tienda_id", IntegerType(), False),
    StructField("empleado_id", IntegerType(), False),
    StructField("cliente_id", IntegerType(), True),
    StructField("sku", StringType(), False),
    StructField("cantidad", IntegerType(), False),
    StructField("precio_unitario", DecimalType(18,2), False),
    StructField("descuento_pct", IntegerType(), True),
    StructField("medio_pago", StringType(), False),
    StructField("venta_total", DecimalType(18,2), False),
    StructField("fecha_hora", TimestampType(), False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import concat, date_format

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_with_date_and_time = df_ventas_validos.withColumn(
    "fecha_hora",
    to_timestamp(
        concat(
            date_format("fecha", "yyyy-MM-dd"),
            lit(" "),
            date_format("hora", "HH:mm:ss")
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_rechazados_with_date_and_time = df_rechazados.withColumn(
    "fecha_hora",
    to_timestamp(
        concat(
            date_format("fecha", "yyyy-MM-dd"),
            lit(" "),
            date_format("hora", "HH:mm:ss")
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_dropped = df_ventas_with_date_and_time.drop(
    df_ventas_with_date_and_time["fecha"],
    df_ventas_with_date_and_time["hora"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_rechazados_final = df_ventas_rechazados_with_date_and_time.drop(
    df_ventas_rechazados_with_date_and_time["fecha"],
    df_ventas_rechazados_with_date_and_time["hora"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_final = spark.createDataFrame(
    df_ventas_dropped.rdd,
    schema=ventas_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_final.write.format("delta").mode("overwrite").saveAsTable("silver.ventas_enriquecidas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver.ventas_enriquecidas
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_ventas_rechazados_final.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_rechazados_final.write.format("delta").mode("overwrite").saveAsTable("silver.rechazos_ventas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver.rechazos_ventas
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
