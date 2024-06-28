import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta

# Parâmetros
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Data de ontem
data_ontem = datetime.now() - timedelta(days=1)
prefixo_s3 = f"s3://myprontuariobucket/13/2024/{data_ontem.month}/{data_ontem.day}/"

# Ler dados do S3
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [prefixo_s3]},
    format="parquet", # ou outro formato
    transformation_ctx="datasource"
)

# Exemplo de transformação - você pode ajustar conforme necessário
# Exemplo: Selecionar apenas algumas colunas e salvar no destino
applymapping1 = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ("nome_coluna_s3", "string", "nome_coluna_destino", "string"),
        # Adicione mais mapeamentos conforme necessário
    ],
    transformation_ctx="applymapping1"
)

# Escrever os dados transformados no destino
# Exemplo: Salvar em outro diretório no S3
output_path = "s3://myprontuariobucket/36/2024/"
glueContext.write_dynamic_frame.from_options(
    frame=applymapping1,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet", # ou outro formato
    transformation_ctx="datasink"
)

# Executar o job
job.commit()
