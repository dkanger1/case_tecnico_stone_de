"""
Utilitários compartilhados para DAGs de transformação Gold.
Centraliza lógica comum de criação de tabelas gold (agregações e análises).
"""

import duckdb
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Configuração centralizada
DB_PATH = "/opt/airflow/data/meu_data_warehouse.db"


class GoldTransformationError(Exception):
    """Exceção customizada para erros de transformação gold."""
    pass


def get_duckdb_connection(db_path: str = DB_PATH):
    """
    Cria conexão com DuckDB.
    
    Args:
        db_path: Caminho do arquivo DuckDB
        
    Returns:
        Conexão DuckDB
    """
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        logger.info(f"📁 Diretório criado: {os.path.dirname(db_path)}")
    
    return duckdb.connect(db_path)


def verify_silver_tables_exist(
    silver_tables: List[str],
    db_path: str = DB_PATH
) -> Dict[str, Any]:
    """
    Verifica se todas as tabelas silver existem e têm dados.
    
    Args:
        silver_tables: Lista de nomes de tabelas silver
        db_path: Caminho do arquivo DuckDB
        
    Returns:
        Dicionário com status de verificação
        
    Raises:
        GoldTransformationError: Se alguma tabela não existir ou estiver vazia
    """
    if not os.path.exists(db_path):
        raise GoldTransformationError(f"Banco de dados não encontrado: {db_path}")
    
    con = get_duckdb_connection(db_path)
    try:
        logger.info(f"🔍 Verificando tabelas silver: {', '.join(silver_tables)}")
        
        tables = con.execute("SHOW TABLES").fetchdf()
        existing_tables = tables['name'].values.tolist() if len(tables) > 0 else []
        
        missing_tables = []
        empty_tables = []
        table_counts = {}
        
        for table in silver_tables:
            if table not in existing_tables:
                missing_tables.append(table)
            else:
                count_result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                count = count_result[0] if count_result else 0
                table_counts[table] = count
                
                if count == 0:
                    empty_tables.append(table)
        
        if missing_tables:
            raise GoldTransformationError(
                f"Tabelas silver não encontradas: {missing_tables}. "
                f"Execute as DAGs silver primeiro."
            )
        
        if empty_tables:
            raise GoldTransformationError(
                f"Tabelas silver vazias: {empty_tables}. "
                f"Verifique as transformações silver."
            )
        
        result = {
            "status": "success",
            "tables_verified": silver_tables,
            "table_counts": table_counts,
            "verified_at": datetime.utcnow().isoformat()
        }
        
        logger.info(f"✅ Todas as tabelas silver verificadas com sucesso!")
        for table, count in table_counts.items():
            logger.info(f"   - {table}: {count:,} linhas")
        
        return result
        
    except GoldTransformationError:
        raise
    except Exception as e:
        error_msg = f"Erro ao verificar tabelas silver: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise GoldTransformationError(error_msg)
    finally:
        con.close()


def create_gold_table(
    query: str,
    table_name: str,
    db_path: str = DB_PATH,
    if_exists: str = "replace",
    add_metadata: bool = True
) -> Dict[str, Any]:
    """
    Cria tabela gold no DuckDB usando uma query SQL.
    
    Args:
        query: Query SQL para criar a tabela gold
        table_name: Nome da tabela gold
        db_path: Caminho do arquivo DuckDB
        if_exists: Comportamento se tabela existir ('replace', 'append', 'fail')
        add_metadata: Se True, adiciona coluna de metadados
        
    Returns:
        Dicionário com informações da criação
        
    Raises:
        GoldTransformationError: Se houver erro na criação
    """
    if not os.path.exists(db_path):
        raise GoldTransformationError(f"Banco de dados não encontrado: {db_path}")
    
    con = get_duckdb_connection(db_path)
    try:
        logger.info(f"💾 Criando tabela gold: {table_name}")
        
        # Construir query com metadados se necessário
        if add_metadata:
            final_query = f"""
            SELECT 
                *,
                CURRENT_TIMESTAMP as _created_at
            FROM ({query})
            """
        else:
            final_query = query
        
        # Verificar se tabela existe
        tables = con.execute("SHOW TABLES").fetchdf()
        table_exists = table_name in tables['name'].values if len(tables) > 0 else False
        
        if table_exists and if_exists == "fail":
            raise GoldTransformationError(f"Tabela {table_name} já existe e if_exists='fail'")
        
        # Criar ou substituir tabela
        if if_exists == "replace":
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {final_query}")
            logger.info(f"✅ Tabela {table_name} criada/substituída")
        elif if_exists == "append":
            if table_exists:
                con.execute(f"INSERT INTO {table_name} {final_query}")
                logger.info(f"✅ Dados inseridos na tabela {table_name}")
            else:
                con.execute(f"CREATE TABLE {table_name} AS {final_query}")
                logger.info(f"✅ Tabela {table_name} criada")
        else:
            con.execute(f"CREATE TABLE {table_name} AS {final_query}")
            logger.info(f"✅ Tabela {table_name} criada")
        
        # Verificar tabela criada
        count_result = con.execute(f"SELECT COUNT(*) as total FROM {table_name}").fetchone()
        total_rows = count_result[0] if count_result else 0
        
        # Obter schema
        schema_result = con.execute(f"DESCRIBE {table_name}").fetchdf()
        columns = schema_result['column_name'].tolist()
        
        result = {
            "status": "success",
            "table_name": table_name,
            "total_rows_in_table": int(total_rows),
            "columns": columns,
            "created_at": datetime.utcnow().isoformat(),
            "schema": schema_result.to_dict('records')
        }
        
        logger.info(f"📊 Transformação gold concluída: {result['total_rows_in_table']} linhas -> {table_name}")
        logger.info(f"📋 Colunas: {', '.join(columns)}")
        
        return result
        
    except Exception as e:
        error_msg = f"Erro ao criar tabela gold {table_name}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise GoldTransformationError(error_msg)
    finally:
        con.close()

