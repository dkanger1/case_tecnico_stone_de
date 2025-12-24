from airflow.decorators import dag, task
from datetime import datetime
import logging
from utils.gold_helpers import (
    verify_silver_tables_exist,
    create_gold_table,
    GoldTransformationError
)

logger = logging.getLogger(__name__)

# Configurações
SILVER_TABLES = [
    "silver_crm_user",
    "silver_email_logs",
    "silver_whatsapp_provider"
]

GOLD_TABLE = "fct_attribution"

# Query de transformação gold
# Esta query une todas as tabelas silver em uma visão unificada
# Ajuste conforme suas necessidades de negócio
GOLD_QUERY = """
SELECT ID_USUARIO, EMAIL, NUMERO_TELEFONE , DATA_CONVERSAO,
-- Definindo pesos (○ Peso 1 (Maior): WhatsApp Read, Peso 2 (Médio): Email Click, 3 (Menor): Email Open)
CASE WHEN WPP_DATA.STATUS_ENVIO = 'read' THEN 1
WHEN EMAIL_DATA.TIPO_EVENTO = 'click' THEN 2
WHEN EMAIL_DATA.TIPO_EVENTO = 'open' THEN 3
ELSE 5
END AS PESO_INTERACAO
FROM silver_crm_user USER_DATA
LEFT JOIN silver_email_logs EMAIL_DATA ON USER_DATA.EMAIL = EMAIL_DATA.EMAIL_USUARIO
-- Interações ocorridas nos 7 dias anteriores à conversão email
AND EMAIL_DATA.DATA_EVENTO BETWEEN USER_DATA.DATA_CONVERSAO - INTERVAL '7 DAYS' AND USER_DATA.DATA_CONVERSAO

LEFT JOIN silver_whatsapp_provider WPP_DATA ON USER_DATA.NUMERO_TELEFONE = WPP_DATA.NUMERO_TELEFONE
-- Interações ocorridas nos 7 dias anteriores à conversão wpp
AND WPP_DATA.DATA_ENVIO BETWEEN USER_DATA.DATA_CONVERSAO - INTERVAL '7 DAYS' AND USER_DATA.DATA_CONVERSAO

WHERE DATA_CONVERSAO IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_DATA.ID_USUARIO ORDER BY PESO_INTERACAO ASC, COALESCE(DATA_ENVIO, DATA_EVENTO) DESC) = 1

"""

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['duckdb', 'gold', 'transformation', 'unified'],
    description='Transformação gold: unifica todas as tabelas silver em fct_attribution para responder qual campanha gerou esta conversão',
    max_active_tasks=1
)
def gold_fct_attribution_transformation():

    @task
    def verify_silvers():
        """
        Task que verifica se todas as tabelas silver existem e têm dados.
        """
        try:
            result = verify_silver_tables_exist(
                silver_tables=SILVER_TABLES
            )
            
            success_msg = (
                f"✅ Todas as tabelas silver verificadas: {', '.join(SILVER_TABLES)}"
            )
            logger.info(success_msg)
            for table, count in result['table_counts'].items():
                logger.info(f"   - {table}: {count:,} linhas")
            
            return success_msg
            
        except GoldTransformationError as e:
            logger.error(f"❌ Erro na verificação das silvers: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {str(e)}")
            raise

    @task
    def create_gold():
        """
        Task que cria a tabela gold unificada.
        """
        try:
            result = create_gold_table(
                query=GOLD_QUERY,
                table_name=GOLD_TABLE,
                if_exists="replace",
                add_metadata=True
            )
            
            success_msg = (
                f"✅ Tabela gold criada: {GOLD_TABLE} "
                f"({result['total_rows_in_table']} linhas)"
            )
            logger.info(success_msg)
            logger.info(f"📋 Colunas: {', '.join(result['columns'])}")
            
            return success_msg
            
        except GoldTransformationError as e:
            logger.error(f"❌ Erro na criação da tabela gold: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {str(e)}")
            raise

    # Fluxo: primeiro verifica silvers, depois cria gold
    verify_task = verify_silvers()
    create_task = create_gold()
    
    # Dependência: gold só executa após verificação das silvers
    verify_task >> create_task

# Instanciando a DAG
dag_exec = gold_fct_attribution_transformation()

