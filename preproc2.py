
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, when, lit
import sys

spark = SparkSession.builder.getOrCreate()

# Obtener periodo desde argumento
periodo = sys.argv[1]

# Lectura de archivos
precios_df = spark.read.option("header", True).option("sep", ";") \
    .csv("s3://aypmd.sources/entrega2/precios_transaccionales.csv")

uf_df = spark.read.option("header", True).option("sep", ",") \
    .csv("s3://aypmd.sources/entrega2/precios_uf.csv")

# Formateo y limpieza
precios_df = precios_df.withColumn(
    "FECHA_NORMALIZADA", to_date(col("FECHA_ESCRITURA").cast("string"), "yyyyMMdd")
)

uf_df = uf_df.withColumn(
    "FECHA_NORMALIZADA", to_date(regexp_replace(col("Date"), "T.*", ""), "yyyy-MM-dd")
)

precios_df = precios_df.withColumn("MONTO_PESOS", col("MONTO_PESOS").cast("double")) \
                       .withColumn("MONTO_UF", col("MONTO_UF").cast("double"))

uf_df = uf_df.withColumn("Uf", col("Uf").cast("double"))

# Join y cálculo de campos
df = precios_df.join(uf_df, on="FECHA_NORMALIZADA", how="left")


# Filtros
df = df.filter(col("FECHA_NORMALIZADA").isNotNull())
df = df.filter(
    ~(
        (col("MONTO_PESOS") <= 0) |
        (col("MONTO_UF") <= 0)
    )
)

df = df.dropDuplicates(["ROL"])
df = df.withColumn("periodo", lit(periodo))

# Escritura final
df.write.mode("overwrite").partitionBy("periodo").parquet("s3://grupo5-aypmd/entrega2/tablas_precios_transaccionales/")
