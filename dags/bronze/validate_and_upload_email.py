from airflow.decorators import dag, task
from datetime import datetime
import logging
import csv
import json
import pandas as pd
import duckdb
import os

logger = logging.getLogger(__name__)

# Configurações do arquivo e tabela
FILE_CONFIG = {
    "file": "/opt/airflow/data/raw_sfmc_email_logs.csv",
    "table": "email_logs"
}

# Caminho do banco DuckDB
DB_PATH = "/opt/airflow/data/meu_data_warehouse.db"

def validate_and_ingest_to_duckdb():
    """
    Função simples que valida JSON e faz ingestão no DuckDB.
    - Lê o CSV linha por linha
    - Valida o campo message_details (JSON)
    - Loga linhas com JSON inválido
    - Insere apenas linhas válidas no DuckDB
    """
    valid_rows = []
    invalid_count = 0
    invalid_lines = []

    # Ler CSV e validar JSON
    logger.info(f"📖 Lendo arquivo: {FILE_CONFIG['file']}")
    
    with open(FILE_CONFIG['file'], newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        for line_number, row in enumerate(reader, start=2):  # start=2 porque linha 1 é header
            message_details = row.get("message_details", "")
            
            # Validar JSON
            try:
                # Verificar se está vazio ou nulo
                if not message_details or message_details.strip() == "":
                    invalid_count += 1
                    invalid_lines.append({
                        'line': line_number,
                        'reason': 'Campo message_details está vazio',
                        'value': message_details[:100] if message_details else 'NULL'
                    })
                    logger.error(f"Linha {line_number}: Campo message_details está vazio")
                    continue
                
                # Tentar parsear JSON
                json.loads(message_details)
                valid_rows.append(row)
                
            except json.JSONDecodeError as e:
                invalid_count += 1
                invalid_lines.append({
                    'line': line_number,
                    'reason': f'JSON inválido: {str(e)}',
                    'value': message_details[:200]  # Primeiros 200 caracteres
                })
                logger.error(f"Linha {line_number}: JSON inválido - {str(e)}")
                logger.error(f"Valor: {message_details[:200]}")
                
            except Exception as e:
                invalid_count += 1
                invalid_lines.append({
                    'line': line_number,
                    'reason': f'Erro inesperado: {str(e)}',
                    'value': message_details[:200]
                })
                logger.error(f"❌ Linha {line_number}: Erro - {str(e)}")
    
    # Log de resumo
    total_rows = len(valid_rows) + invalid_count
    logger.info(f"Resumo da validação:")
    logger.info(f"Linhas válidas: {len(valid_rows):,}")
    logger.info(f"Linhas rejeitadas: {invalid_count:,}")
    logger.info(f"Total processado: {total_rows:,} linhas")
    
    if invalid_count > 0:
        logger.warning(f"QUALITY GATE: {invalid_count} linhas foram rejeitadas devido a JSON inválido")
        # Log detalhado das primeiras 10 linhas inválidas
        for i, invalid in enumerate(invalid_lines[:10], 1):
            logger.warning(f"   Rejeitada #{i} (linha {invalid['line']}): {invalid['reason']}")
        if len(invalid_lines) > 10:
            logger.warning(f"   ... e mais {len(invalid_lines) - 10} linhas rejeitadas")
    
    # Verificar se há linhas válidas para inserir
    if not valid_rows:
        error_msg = f"FALHOU: Todas as {total_rows} linhas foram rejeitadas. Nenhuma linha será inserida."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Converter para DataFrame
    logger.info(f"Convertendo {len(valid_rows)} linhas válidas para DataFrame...")
    df = pd.DataFrame(valid_rows)
    
    # Adicionar metadados
    df['_ingested_at'] = datetime.utcnow()
    
    # Conectar ao DuckDB e inserir
    logger.info(f"Conectando ao DuckDB: {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    
    try:
        # Criar ou substituir tabela
        logger.info(f"Criando/substituindo tabela: {FILE_CONFIG['table']}")
        con.execute(f"CREATE OR REPLACE TABLE {FILE_CONFIG['table']} AS SELECT * FROM df")
        
        # Verificar inserção
        count_result = con.execute(f"SELECT COUNT(*) FROM {FILE_CONFIG['table']}").fetchone()
        total_inserted = count_result[0] if count_result else 0
        
        logger.info(f"Ingestão concluída com sucesso!")
        logger.info(f"Linhas inseridas: {total_inserted:,}")
        logger.info(f"Tabela: {FILE_CONFIG['table']}")
        if invalid_count > 0:
            logger.info(f"Linhas rejeitadas: {invalid_count:,} (JSON inválido)")        
        return {
            'status': 'success',
            'rows_inserted': total_inserted,
            'rows_rejected': invalid_count,
            'table_name': FILE_CONFIG['table']
        }
        
    finally:
        con.close()


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['duckdb', 'ingestion', 'bronze', 'email', 'sfmc', 'quality-gate'],
    description='Ingestão bronze com validação de qualidade (JSON) message_details e insere apenas linhas válidas no DuckDB',
    max_active_tasks=1
)
def ingestion_sfmc_email_logs_validate():

    @task
    def ingest_email_logs():
        """
        Task de ingestão do arquivo SFMC Email Logs para a camada bronze.
        Valida campo JSON message_details e rejeita linhas inválidas.
        """
        try:
            result = validate_and_ingest_to_duckdb()
            
            success_msg = (
                f"Ingestão concluída: {result['rows_inserted']} linhas inseridas "
                f"em {result['table_name']}"
            )
            if result['rows_rejected'] > 0:
                success_msg += f" | {result['rows_rejected']} linhas rejeitadas (JSON inválido)"
            
            return success_msg
            
        except Exception as e:
            logger.error(f"Erro na ingestão: {str(e)}")
            raise

    # Executar task
    ingest_task = ingest_email_logs()

# Instanciando a DAG
dag_exec = ingestion_sfmc_email_logs_validate()
