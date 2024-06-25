import boto3
import time
import pandas as pd

athena_client = boto3.client('athena')

# Iniciar a execução da consulta com um OutputLocation temporário
response = athena_client.start_query_execution(
    QueryString='SELECT * FROM prontuario_db."prontuario-data";',
    QueryExecutionContext={
        'Database': 'prontuario_db'
    },
    ResultConfiguration={
        'OutputLocation': 's3://myathenauserstack-queryresultsbucketa33a3e24-0wo3odribybo/results/'
    }
)

query_execution_id = response['QueryExecutionId']

# Esperar a consulta ser concluída
while True:
    response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    status = response['QueryExecution']['Status']['State']
    if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        break
    time.sleep(2)

if status == 'SUCCEEDED':
    # Obter os resultados da consulta com paginação
    next_token = None
    rows = []
    columns = []

    while True:
        if next_token:
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token)
        else:
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

        if not columns:
            column_info = results['ResultSet']['ResultSetMetadata']['ColumnInfo']
            columns = [col['Name'] for col in column_info]

        for row in results['ResultSet']['Rows'][1:]: 
            rows.append([col.get('VarCharValue', '') for col in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    # Converter os resultados para um DataFrame do pandas e imprimir
    df = pd.DataFrame(rows, columns=columns)
    print(df)
elif status == 'FAILED':
    # Obter detalhes do erro
    reason = response['QueryExecution']['Status']['StateChangeReason']
    print(f"Query failed with status: {status}.\nReason: {reason}")
else:
    print(f"Query was cancelled with status: {status}")
