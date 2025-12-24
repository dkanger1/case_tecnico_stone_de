from airflow.decorators import dag, task
from datetime import datetime
import logging
from utils.bronze_helpers import ingest_csv_to_bronze, BronzeIngestionError

logger = logging.getLogger(__name__)

# Configurações do arquivo e tabela
FILE_CONFIG = {
    "file": "/opt/airflow/data/raw_whatsapp_provider.csv",
    "table": "whatsapp_provider"
}

# Colunas esperadas (opcional - descomente e ajuste conforme necessário)
# EXPECTED_COLUMNS = ["message_id", "user_id", "sent_at", "status"]

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['duckdb', 'ingestion', 'bronze', 'whatsapp'],
    description='Ingestão bronze: WhatsApp Provider CSV -> tabela whatsapp_provider no DuckDB',
    max_active_tasks=1
)
def ingestion_whatsapp_provider():

    @task
    def ingest_whatsapp_provider():
        """
        Task de ingestão do arquivo WhatsApp Provider para a camada bronze.
        """
        try:
            result = ingest_csv_to_bronze(
                file_path=FILE_CONFIG["file"],
                table_name=FILE_CONFIG["table"],
                expected_columns=None,  # Ajuste conforme necessário
                min_rows=0,  # Ajuste conforme necessário
                if_exists="replace",
                add_metadata=True
            )
            
            success_msg = (
                f"✅ Ingestão concluída: {result['rows_ingested']} linhas "
                f"-> {result['table_name']} (Total: {result['total_rows_in_table']} linhas)"
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
    ingest_task = ingest_whatsapp_provider()

# Instanciando a DAG
dag_exec = ingestion_whatsapp_provider()