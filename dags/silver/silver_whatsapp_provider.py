from airflow.decorators import dag, task
from datetime import datetime
import logging
from utils.silver_helpers import transform_bronze_to_silver, SilverTransformationError

logger = logging.getLogger(__name__)

# Configurações
BRONZE_TABLE = "whatsapp_provider"
SILVER_TABLE = "silver_whatsapp_provider"

# Query de transformação silver
# Ajuste esta query conforme suas necessidades de transformação
SILVER_QUERY = """
SELECT message_id as ID_MENSAGEM,
CAST(phone_clean as VARCHAR) as NUMERO_TELEFONE,
CAST(sent_at_brt AS TIMESTAMP) AT TIME ZONE 'America/Sao_Paulo' AS DATA_ENVIO,
status as STATUS_ENVIO,
TRIM(REPLACE(REGEXP_EXTRACT(campaign_tag, '-\s*([^-\s]+)\s*-'), '-', '')) as TAG_CAMPANHA
FROM {bronze_table}
"""

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['duckdb', 'silver', 'transformation', 'whatsapp'],
    description='Transformação silver: whatsapp_provider bronze -> silver_whatsapp_provider',
    max_active_tasks=1
)
def silver_whatsapp_provider_transformation():

    @task
    def transform_whatsapp_provider():
        """
        Task de transformação do bronze whatsapp_provider para silver.
        """
        try:
            result = transform_bronze_to_silver(
                bronze_table=BRONZE_TABLE,
                silver_query=SILVER_QUERY,
                silver_table=SILVER_TABLE,
                test_first=True,
                if_exists="replace",
                add_metadata=True
            )
            
            success_msg = (
                f"✅ Transformação silver concluída: {BRONZE_TABLE} -> {SILVER_TABLE} "
                f"({result['total_rows_in_table']} linhas)"
            )
            logger.info(success_msg)
            return success_msg
            
        except SilverTransformationError as e:
            logger.error(f"❌ Erro na transformação silver: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {str(e)}")
            raise

    # Executar task
    transform_task = transform_whatsapp_provider()

# Instanciando a DAG
dag_exec = silver_whatsapp_provider_transformation()

