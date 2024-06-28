import sys
from pyspark.sql.functions import col, concat_ws, collect_list, flatten, expr
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leia os dados do Glue Catalog
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="prontuario_db",
    table_name="prontuario-data-2024"
)

# Converter DynamicFrame para DataFrame para usar funções do Spark
df = datasource0.toDF()

# Mostrar esquema e algumas linhas para verificar a estrutura dos dados
df.printSchema()
df.show(5, truncate=False)

# Extraindo os campos do JSON na coluna "event"
df_extracted = df \
    .withColumn("timelineid", col("timelineid")) \
    .withColumn("eventid", col("eventid")) \
    .withColumn("eventtypename", col("eventtypename")) \
    .withColumn("eventtypeversion", col("eventtypeversion")) \
    .withColumn("ownerid", col("ownerid")) \
    .withColumn("createdby", col("createdby")) \
    .withColumn("tenantid", col("tenantid")) \
    .withColumn("createdat", col("createdat")) \
    .withColumn("updatedat", col("updatedat")) \
    .withColumn("partition_0", col("partition_0")) \
    .withColumn("partition_1", col("partition_1")) \
    .withColumn("information", col("event").getField("information")) \
    .withColumn("complaintduration", col("event").getField("complaintduration")) \
    .withColumn("recordappointmenttype_id", col("event").getField("recordappointmenttype").getField("id")) \
    .withColumn("recordappointmenttype_name", col("event").getField("recordappointmenttype").getField("name")) \
    .withColumn("resolution_id", col("event").getField("resolution").getField("id")) \
    .withColumn("resolution_name", col("event").getField("resolution").getField("name")) \
    .withColumn("severity_id", col("event").getField("severity").getField("id")) \
    .withColumn("severity_name", col("event").getField("severity").getField("name")) \
    .withColumn("complaints_id", col("event").getField("complaints").getField("id")) \
    .withColumn("complaints_name", col("event").getField("complaints").getField("name")) \
    .withColumn("diagnosis_book", col("event").getField("diagnosis").getField("book")) \
    .withColumn("diagnosis_version", col("event").getField("diagnosis").getField("version")) \
    .withColumn("diagnosis_chapter_code", col("event").getField("diagnosis").getField("chapter").getField("code")) \
    .withColumn("diagnosis_chapter_title", col("event").getField("diagnosis").getField("chapter").getField("title")) \
    .withColumn("diagnosis_block_code", col("event").getField("diagnosis").getField("block").getField("code")) \
    .withColumn("diagnosis_block_title", col("event").getField("diagnosis").getField("block").getField("title")) \
    .withColumn("diagnosis_leaf_code", col("event").getField("diagnosis").getField("leaf").getField("code")) \
    .withColumn("diagnosis_leaf_title", col("event").getField("diagnosis").getField("leaf").getField("title")) \
    .withColumn("diagnostichypothesis_book", col("event").getField("diagnostichypothesis").getField("book")) \
    .withColumn("diagnostichypothesis_version", col("event").getField("diagnostichypothesis").getField("version")) \
    .withColumn("diagnostichypothesis_chapter_code", col("event").getField("diagnostichypothesis").getField("chapter").getField("code")) \
    .withColumn("diagnostichypothesis_chapter_title", col("event").getField("diagnostichypothesis").getField("chapter").getField("title")) \
    .withColumn("diagnostichypothesis_block_code", col("event").getField("diagnostichypothesis").getField("block").getField("code")) \
    .withColumn("diagnostichypothesis_block_title", col("event").getField("diagnostichypothesis").getField("block").getField("title")) \
    .withColumn("diagnostichypothesis_leaf_code", col("event").getField("diagnostichypothesis").getField("leaf").getField("code")) \
    .withColumn("diagnostichypothesis_leaf_title", col("event").getField("diagnostichypothesis").getField("leaf").getField("title"))

# Remover a coluna original de evento JSON
df_cleaned = df_extracted.drop("event")

# Agrupar por chaves primárias e coletar listas para manter estrutura original
df_grouped = df_cleaned.groupBy(
    "timelineid", "eventid", "eventtypename", "eventtypeversion", "ownerid", "createdby", "tenantid", 
    "createdat", "updatedat", "sharing", "partition_0", "partition_1", "information", "complaintduration", 
    "recordappointmenttype_id", "recordappointmenttype_name", "resolution_id", "resolution_name", 
    "severity_id", "severity_name"
).agg(
    concat_ws(", ", flatten(collect_list("complaints_id"))).alias("complaints_id_list"),
    concat_ws(", ", flatten(collect_list("complaints_name"))).alias("complaints_name_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_book"))).alias("diagnosis_book_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_version"))).alias("diagnosis_version_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_chapter_code"))).alias("diagnosis_chapter_code_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_chapter_title"))).alias("diagnosis_chapter_title_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_block_code"))).alias("diagnosis_block_code_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_block_title"))).alias("diagnosis_block_title_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_leaf_code"))).alias("diagnosis_leaf_code_list"),
    concat_ws(", ", flatten(collect_list("diagnosis_leaf_title"))).alias("diagnosis_leaf_title_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_book"))).alias("diagnostichypothesis_book_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_version"))).alias("diagnostichypothesis_version_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_chapter_code"))).alias("diagnostichypothesis_chapter_code_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_chapter_title"))).alias("diagnostichypothesis_chapter_title_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_block_code"))).alias("diagnostichypothesis_block_code_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_block_title"))).alias("diagnostichypothesis_block_title_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_leaf_code"))).alias("diagnostichypothesis_leaf_code_list"),
    concat_ws(", ", flatten(collect_list("diagnostichypothesis_leaf_title"))).alias("diagnostichypothesis_leaf_title_list")
)

# Obter o esquema atual para determinar quais colunas possuem arrays
schema = df_grouped.schema

# Filtrar colunas que não são arrays
non_array_columns = [field.name for field in schema.fields if not field.dataType.typeName() == 'ArrayType']

# Selecionar apenas as colunas que não são arrays
df_filtered = df_grouped.select(non_array_columns)

# Converter DataFrame filtrado de volta para DynamicFrame
datasource_cleaned = DynamicFrame.fromDF(df_filtered, glueContext, "datasource_cleaned")

# Escrever os dados no S3 em formato parquet
datasink = glueContext.write_dynamic_frame.from_options(
    frame=datasource_cleaned,
    connection_type="s3",
    connection_options={"path": "s3://myprontuariobucket/data/"},
    format="parquet"
)

job.commit()
