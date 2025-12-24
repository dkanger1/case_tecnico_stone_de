from airflow.decorators import dag, task
from datetime import datetime
import logging
from utils.silver_helpers import transform_bronze_to_silver, SilverTransformationError

logger = logging.getLogger(__name__)

# Configurações
BRONZE_TABLE = "email_logs"
SILVER_TABLE = "silver_email_logs"

# Query de transformação silver
# Ajuste esta query conforme suas necessidades de transformação
SILVER_QUERY = """
SELECT event_id AS ID_EVENTO,
lower(user_email) as EMAIL_USUARIO,
CAST(event_timestamp AS TIMESTAMP) AT TIME ZONE 'America/Sao_Paulo' AS DATA_EVENTO,
event_type as TIPO_EVENTO,
REPLACE(json_extract(message_details, '$.campaign_code'), '"','') AS CODIGO_CAMPANHA
--json_extract(message_details, '$.journey_id') AS journey_id,
--json_extract(message_details, '$.template') AS template,
FROM {bronze_table}
WHERE
json_valid(message_details) = TRUE
"""

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['duckdb', 'silver', 'transformation', 'email', 'sfmc'],
    description='Transformação silver: email_logs bronze -> silver_email_logs',
    max_active_tasks=1
)
def silver_email_logs_transformation():

    @task
    def transform_email_logs():
        """
        Task de transformação do bronze email_logs para silver.
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
    transform_task = transform_email_logs()

# Instanciando a DAG
dag_exec = silver_email_logs_transformation()

