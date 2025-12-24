from airflow.decorators import dag, task
from datetime import datetime
import logging
from utils.bronze_helpers import ingest_csv_to_bronze, BronzeIngestionError

logger = logging.getLogger(__name__)

# Configurações do arquivo e tabela
FILE_CONFIG = {
    "file": "/opt/airflow/data/raw_sfmc_email_logs.csv",
    "table": "email_logs"
}

# Colunas esperadas (opcional - descomente e ajuste conforme necessário)
# EXPECTED_COLUMNS = ["email_id", "user_id", "sent_at", "status"]

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['duckdb', 'ingestion', 'bronze', 'email', 'sfmc'],
    description='Ingestão bronze: SFMC Email Logs CSV -> tabela email_logs no DuckDB',
    max_active_tasks=1
)
def ingestion_sfmc_email_logs():

    @task
    def ingest_email_logs():
        """
        Task de ingestão do arquivo SFMC Email Logs para a camada bronze.
        Inclui Quality Gate para validar campo JSON message_details.
        """
        try:
            result = ingest_csv_to_bronze(
                file_path=FILE_CONFIG["file"],
                table_name=FILE_CONFIG["table"],
                expected_columns=None,  # Ajuste conforme necessário
                min_rows=0,  # Ajuste conforme necessário
                if_exists="replace",
                add_metadata=True,
                validate_json="message_details"  # QUALITY GATE: valida JSON
            )
            
            # Mensagem de sucesso com informações de validação
            success_msg = (
                f"✅ Ingestão concluída: {result['rows_ingested']} linhas "
                f"-> {result['table_name']} (Total: {result['total_rows_in_table']} linhas)"
            )
            
            # Adicionar informações sobre linhas rejeitadas se houver
            if 'json_validation' in result:
                json_stats = result['json_validation']
                rejected_count = json_stats.get('rejected_rows', 0)
                if rejected_count > 0:
                    success_msg += (
                        f"\n⚠️ QUALITY GATE: {rejected_count} linhas rejeitadas "
                        f"(JSON inválido em message_details)"
                    )
                    logger.warning(
                        f"QUALITY GATE: {rejected_count} linhas com JSON inválido foram rejeitadas "
                        f"e não foram inseridas no banco."
                    )
            
            logger.info(success_msg)
            return success_msg
            
        except BronzeIngestionError as e:
            logger.error(f"❌ Erro na ingestão bronze: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {str(e)}")
            raise

    # Executar task
    ingest_task = ingest_email_logs()

# Instanciando a DAG
dag_exec = ingestion_sfmc_email_logs()