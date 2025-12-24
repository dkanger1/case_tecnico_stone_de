from airflow.decorators import dag, task
from datetime import datetime
import logging
from utils.silver_helpers import transform_bronze_to_silver, SilverTransformationError

logger = logging.getLogger(__name__)

# Configurações
BRONZE_TABLE = "crm_user"
SILVER_TABLE = "silver_crm_user"

# Query de transformação silver
# Ajuste esta query conforme suas necessidades de transformação
SILVER_QUERY = """
SELECT user_id as ID_USUARIO,
lower(email) as EMAIL,
CAST(REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS VARCHAR) AS NUMERO_TELEFONE,
CAST(conversion_at AS TIMESTAMP) AT TIME ZONE 'America/Sao_Paulo' AS DATA_CONVERSAO
FROM {bronze_table}
"""

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['duckdb', 'silver', 'transformation', 'crm'],
    description='Transformação silver: crm_user bronze -> silver_crm_user',
    max_active_tasks=1
)
def silver_crm_user_transformation():

    @task
    def transform_crm_user():
        """
        Task de transformação do bronze crm_user para silver.
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
    transform_task = transform_crm_user()

# Instanciando a DAG
dag_exec = silver_crm_user_transformation()

