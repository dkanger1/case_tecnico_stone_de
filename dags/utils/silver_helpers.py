"""
Utilitários compartilhados para DAGs de transformação Silver.
Centraliza lógica comum de transformação e criação de tabelas silver.
"""

import duckdb
import os
from datetime import datetime
from typing import Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Configuração centralizada
DB_PATH = "/opt/airflow/data/meu_data_warehouse.db"


class SilverTransformationError(Exception):
    """Exceção customizada para erros de transformação silver."""
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


def test_silver_query(query: str, db_path: str = DB_PATH, limit: int = 10) -> Dict[str, Any]:
    """
    Testa uma query SQL antes de criar a tabela silver.
    
    Args:
        query: Query SQL para testar
        db_path: Caminho do arquivo DuckDB
        limit: Número de linhas para retornar na amostra
        
    Returns:
        Dicionário com informações do teste
        
    Raises:
        SilverTransformationError: Se a query falhar
    """
    if not os.path.exists(db_path):
        raise SilverTransformationError(f"Banco de dados não encontrado: {db_path}")
    
    con = get_duckdb_connection(db_path)
    try:
        logger.info("🔍 Testando query silver...")
        
        # 1. Testar a query com LIMIT
        test_query_with_limit = f"SELECT * FROM ({query}) LIMIT {limit}"
        result = con.execute(test_query_with_limit).fetchdf()
        
        # 2. Contar total de linhas
        count_query = f"SELECT COUNT(*) as total FROM ({query})"
        count_result = con.execute(count_query).fetchone()
        total_rows = count_result[0] if count_result else 0
        
        # 3. Obter schema
        schema_query = f"DESCRIBE SELECT * FROM ({query}) LIMIT 1"
        schema_result = con.execute(schema_query).fetchdf()
        
        result_dict = {
            "status": "success",
            "total_rows": int(total_rows),
            "sample_rows": int(len(result)),
            "sample_data": result.to_dict('records'),
            "schema": schema_result.to_dict('records'),
            "query_tested": query
        }
        
        logger.info(f"✅ Query testada com sucesso!")
        logger.info(f"📊 Total de linhas: {total_rows}")
        logger.info(f"📋 Schema: {schema_result.to_dict('records')}")
        
        return result_dict
        
    except Exception as e:
        error_msg = f"Erro ao testar query: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise SilverTransformationError(error_msg)
    finally:
        con.close()


def create_silver_table(
    query: str,
    table_name: str,
    db_path: str = DB_PATH,
    test_result: Optional[Dict[str, Any]] = None,
    if_exists: str = "replace",
    add_metadata: bool = True
) -> Dict[str, Any]:
    """
    Cria tabela silver no DuckDB usando uma query SQL.
    
    Args:
        query: Query SQL para criar a tabela
        table_name: Nome da tabela silver
        db_path: Caminho do arquivo DuckDB
        test_result: Resultado do teste da query (opcional, para validação)
        if_exists: Comportamento se tabela existir ('replace', 'append', 'fail')
        add_metadata: Se True, adiciona coluna de metadados
        
    Returns:
        Dicionário com informações da criação
        
    Raises:
        SilverTransformationError: Se houver erro na criação
    """
    if test_result and test_result.get("status") != "success":
        raise SilverTransformationError(
            "A query não foi testada com sucesso. Não é possível criar a tabela."
        )
    
    if not os.path.exists(db_path):
        raise SilverTransformationError(f"Banco de dados não encontrado: {db_path}")
    
    con = get_duckdb_connection(db_path)
    try:
        logger.info(f"💾 Criando tabela silver: {table_name}")
        
        # Construir query com metadados se necessário
        if add_metadata:
            # Adicionar coluna de metadados na query
            final_query = f"""
            SELECT 
                *,
                CURRENT_TIMESTAMP as _transformed_at
            FROM ({query})
            """
        else:
            final_query = query
        
        # Verificar se tabela existe
        tables = con.execute("SHOW TABLES").fetchdf()
        table_exists = table_name in tables['name'].values if len(tables) > 0 else False
        
        if table_exists and if_exists == "fail":
            raise SilverTransformationError(f"Tabela {table_name} já existe e if_exists='fail'")
        
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
            "transformed_at": datetime.utcnow().isoformat(),
            "schema": schema_result.to_dict('records')
        }
        
        logger.info(f"📊 Transformação silver concluída: {result['total_rows_in_table']} linhas -> {table_name}")
        logger.info(f"📋 Colunas: {', '.join(columns)}")
        
        return result
        
    except Exception as e:
        error_msg = f"Erro ao criar tabela silver {table_name}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise SilverTransformationError(error_msg)
    finally:
        con.close()


def transform_bronze_to_silver(
    bronze_table: str,
    silver_query: str,
    silver_table: str,
    db_path: str = DB_PATH,
    test_first: bool = True,
    if_exists: str = "replace",
    add_metadata: bool = True
) -> Dict[str, Any]:
    """
    Função completa de transformação bronze para silver.
    Testa a query e cria a tabela silver.
    
    Args:
        bronze_table: Nome da tabela bronze de origem
        silver_query: Query SQL para transformação (pode referenciar {bronze_table})
        silver_table: Nome da tabela silver de destino
        db_path: Caminho do arquivo DuckDB
        test_first: Se True, testa a query antes de criar a tabela
        if_exists: Comportamento se tabela existir
        add_metadata: Adicionar colunas de metadados
        
    Returns:
        Dicionário com informações da transformação
    """
    logger.info(f"🚀 Iniciando transformação silver: {bronze_table} -> {silver_table}")
    
    # Substituir placeholder {bronze_table} na query se existir
    if "{bronze_table}" in silver_query:
        silver_query = silver_query.format(bronze_table=bronze_table)
    
    # Verificar se tabela bronze existe
    con = get_duckdb_connection(db_path)
    try:
        tables = con.execute("SHOW TABLES").fetchdf()
        bronze_exists = bronze_table in tables['name'].values if len(tables) > 0 else False
        
        if not bronze_exists:
            raise SilverTransformationError(
                f"Tabela bronze '{bronze_table}' não encontrada. "
                f"Execute a DAG de ingestão bronze primeiro."
            )
        
        bronze_count = con.execute(f"SELECT COUNT(*) FROM {bronze_table}").fetchone()[0]
        logger.info(f"📊 Tabela bronze '{bronze_table}' encontrada com {bronze_count} linhas")
    finally:
        con.close()
    
    # Testar query se solicitado
    test_result = None
    if test_first:
        logger.info("🧪 Testando query antes de criar tabela...")
        test_result = test_silver_query(silver_query, db_path)
    
    # Criar tabela silver
    result = create_silver_table(
        query=silver_query,
        table_name=silver_table,
        db_path=db_path,
        test_result=test_result,
        if_exists=if_exists,
        add_metadata=add_metadata
    )
    
    logger.info(f"✅ Transformação silver concluída com sucesso!")
    return result

