from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
import sys

spark = SparkSession.builder.getOrCreate()
periodo = sys.argv[1]

# 1. Precios Transaccionales

precios_df = spark.read.option("header", True).option("sep", ";") \
    .csv("s3://aypmd.sources/entrega2/precios_transaccionales.csv")

uf_df = spark.read.option("header", True).option("sep", ",") \
    .csv("s3://aypmd.sources/entrega2/precios_uf.csv")

precios_df = precios_df.withColumn(
    "FECHA_NORMALIZADA", to_date(col("FECHA_ESCRITURA").cast("string"), "yyyyMMdd")
)

uf_df = uf_df.withColumn(
    "FECHA_NORMALIZADA", to_date(regexp_replace(col("Date"), "T.*", ""), "yyyy-MM-dd")
)

precios_df = precios_df.withColumn("MONTO_PESOS", col("MONTO_PESOS").cast("double")) \
                       .withColumn("MONTO_UF", col("MONTO_UF").cast("double"))

uf_df = uf_df.withColumn("Uf", col("Uf").cast("double"))

df_precios = precios_df.join(uf_df, on="FECHA_NORMALIZADA", how="left")
df_precios = df_precios.filter(col("FECHA_NORMALIZADA").isNotNull())
df_precios = df_precios.filter(
    ~((col("MONTO_PESOS") <= 0) | (col("MONTO_UF") <= 0))
)
df_precios = df_precios.dropDuplicates(["ROL"]).withColumn("periodo", lit(periodo))

df_precios.write.mode("overwrite").partitionBy("periodo") \
    .parquet("s3://grupo5-aypmd/entrega2/tablas_precios_transaccionales/")

# 2. Construcciones y No Agrícola

file_path = f"s3://aypmd.sources/entrega2/construcciones/BRORGA2441NL_NAC_{periodo}.gz"
file_path_no_agricola = f"s3://aypmd.sources/entrega2/no_agricola/BRORGA2441N_NAC_{periodo}.gz"

schema = StructType([
    StructField("cod_com", IntegerType()),
    StructField("cod_mz", IntegerType()),
    StructField("cod_pr", IntegerType()),
    StructField("numero_linea", IntegerType()),
    StructField("cod_material", StringType()),
    StructField("cod_calidad", IntegerType()),
    StructField("ano_construccion", IntegerType()),
    StructField("superficie_construida", IntegerType()),
    StructField("cod_destino", StringType()),
    StructField("cod_cond_esp", StringType()),
    StructField("col_extra", StringType()),
    StructField("col_extra1", StringType())
])

schema_no_agricola = StructType([
    StructField("cod_com", IntegerType()),
    StructField("cod_mz", IntegerType()),
    StructField("cod_pr", IntegerType()),
    StructField("direccion", StringType()),
    StructField("avaluo_fiscal_clp", IntegerType()),
    StructField("contrib_semestral_clp", IntegerType()),
    StructField("cod_destino", StringType()),
    StructField("avaluo_exento_clp", IntegerType()),
    StructField("cod_com_bc", IntegerType()),
    StructField("cod_mz_bc", IntegerType()),
    StructField("cod_pr_bc", IntegerType()),
    StructField("cod_com_bc2", IntegerType()),
    StructField("cod_mz_bc2", IntegerType()),
    StructField("cod_pr_bc2", IntegerType()),
    StructField("superficie_total_terreno", IntegerType())
])

df_construcciones = spark.read.csv(
    file_path,
    schema=schema,
    header=False,
    sep="|",
    encoding="utf-8"
).select(
    "cod_com", "cod_mz", "cod_pr", "numero_linea", "cod_material",
    "cod_calidad", "ano_construccion", "superficie_construida",
    "cod_destino", "cod_cond_esp"
).withColumn("periodo", F.lit(periodo))

df_construcciones.write \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("periodo") \
    .mode("overwrite") \
    .parquet("s3://grupo5-aypmd/entrega2/tablas_construcciones/")

df_no_agricola = spark.read.csv(
    file_path_no_agricola,
    schema=schema_no_agricola,
    header=False,
    sep="|",
    encoding="utf-8"
).drop(
    "cod_com_bc", "cod_mz_bc", "cod_pr_bc",
    "cod_com_bc2", "cod_mz_bc2", "cod_pr_bc2"
)

df_construcciones_agg = df_construcciones.select(
    "cod_com", "cod_mz", "cod_pr", "superficie_construida", "ano_construccion"
).groupBy("cod_com", "cod_mz", "cod_pr").agg(
    F.sum("superficie_construida").alias("superficie_total_construcciones"),
    F.min("ano_construccion").alias("ano_construccion")
)

df_no_agricola = df_no_agricola.join(
    df_construcciones_agg,
    on=["cod_com", "cod_mz", "cod_pr"],
    how="left"
).withColumn("periodo", F.lit(periodo))

df_no_agricola.write \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("periodo") \
    .mode("overwrite") \
    .parquet("s3://grupo5-aypmd/entrega2/tablas_no_agricola/")

# 3. Nexo BC

df_no_agricola_raw = spark.read.csv(
    file_path_no_agricola,
    schema=schema_no_agricola,
    header=False,
    sep="|",
    encoding="utf-8"
)

df_nexo_bc = df_no_agricola_raw.select(
    "cod_com", "cod_mz", "cod_pr",
    "cod_com_bc", "cod_mz_bc", "cod_pr_bc"
).filter(
    (F.col("cod_com_bc").isNotNull()) & (F.col("cod_pr") < 70000)
).withColumn("periodo", F.lit(periodo))

df_nexo_bc.write \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("periodo") \
    .mode("overwrite") \
    .parquet("s3://grupo5-aypmd/entrega2/tablas_nexo_bc/")
