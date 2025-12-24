"""
Utilitários compartilhados para DAGs de ingestão Bronze.
Centraliza lógica comum e evita duplicação de código.
"""

import os
import pandas as pd
import duckdb
import json
from datetime import datetime
from typing import Dict, Optional, List, Any, Tuple
import logging

logger = logging.getLogger(__name__)

# Configuração centralizada
DB_PATH = "/opt/airflow/data/meu_data_warehouse.db"


class BronzeIngestionError(Exception):
    """Exceção customizada para erros de ingestão bronze."""
    pass


def validate_file_exists(file_path: str) -> None:
    """
    Valida se o arquivo existe e não está vazio.
    
    Args:
        file_path: Caminho do arquivo a validar
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        BronzeIngestionError: Se o arquivo estiver vazio
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    if os.path.getsize(file_path) == 0:
        raise BronzeIngestionError(f"Arquivo está vazio: {file_path}")
    
    logger.info(f"✅ Arquivo validado: {file_path} ({os.path.getsize(file_path)} bytes)")


def read_csv_with_validation(
    file_path: str,
    expected_columns: Optional[List[str]] = None,
    min_rows: int = 0,
    encoding: str = 'utf-8',
    **pandas_kwargs
) -> pd.DataFrame:
    """
    Lê CSV com validações de qualidade de dados.
    
    Args:
        file_path: Caminho do arquivo CSV
        expected_columns: Lista de colunas esperadas (opcional)
        min_rows: Número mínimo de linhas esperadas
        encoding: Encoding do arquivo
        **pandas_kwargs: Argumentos adicionais para pd.read_csv
        
    Returns:
        DataFrame com os dados lidos
        
    Raises:
        BronzeIngestionError: Se validações falharem
    """
    try:
        logger.info(f"📖 Lendo arquivo: {file_path}")
        df = pd.read_csv(file_path, encoding=encoding, **pandas_kwargs)
        
        # Validação: arquivo não vazio
        if len(df) == 0:
            raise BronzeIngestionError(f"Arquivo CSV está vazio: {file_path}")
        
        # Validação: número mínimo de linhas
        if len(df) < min_rows:
            raise BronzeIngestionError(
                f"Arquivo tem apenas {len(df)} linhas, mínimo esperado: {min_rows}"
            )
        
        # Validação: colunas esperadas
        if expected_columns:
            missing_cols = set(expected_columns) - set(df.columns)
            if missing_cols:
                raise BronzeIngestionError(
                    f"Colunas esperadas não encontradas: {missing_cols}. "
                    f"Colunas disponíveis: {list(df.columns)}"
                )
        
        # Log de informações
        logger.info(f"✅ CSV lido com sucesso: {len(df)} linhas, {len(df.columns)} colunas")
        logger.info(f"📋 Colunas: {', '.join(df.columns.tolist())}")
        
        # Estatísticas básicas
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(f"⚠️ Valores nulos encontrados:\n{null_counts[null_counts > 0]}")
        
        return df
        
    except pd.errors.EmptyDataError:
        raise BronzeIngestionError(f"Arquivo CSV está vazio ou corrompido: {file_path}")
    except pd.errors.ParserError as e:
        raise BronzeIngestionError(f"Erro ao parsear CSV: {str(e)}")
    except Exception as e:
        raise BronzeIngestionError(f"Erro inesperado ao ler CSV: {str(e)}")


def get_duckdb_connection(db_path: str = DB_PATH):
    """
    Cria conexão com DuckDB usando context manager.
    
    Args:
        db_path: Caminho do arquivo DuckDB
        
    Returns:
        Conexão DuckDB
    """
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        logger.info(f"📁 Diretório criado: {os.path.dirname(db_path)}")
    
    return duckdb.connect(db_path)


def create_bronze_table(
    df: pd.DataFrame,
    table_name: str,
    db_path: str = DB_PATH,
    if_exists: str = "replace",
    add_metadata: bool = True
) -> Dict[str, Any]:
    """
    Cria tabela bronze no DuckDB com metadados de ingestão.
    
    Args:
        df: DataFrame com os dados
        table_name: Nome da tabela a ser criada
        db_path: Caminho do arquivo DuckDB
        if_exists: Comportamento se tabela existir ('replace', 'append', 'fail')
        add_metadata: Se True, adiciona colunas de metadados
        
    Returns:
        Dicionário com informações da ingestão
        
    Raises:
        BronzeIngestionError: Se houver erro na criação da tabela
    """
    try:
        # Adicionar metadados de ingestão
        if add_metadata:
            df = df.copy()
            df['_ingested_at'] = datetime.utcnow()
            df['_source_file'] = table_name  # Pode ser melhorado para passar o caminho real
        
        logger.info(f"💾 Criando tabela bronze: {table_name}")
        
        con = get_duckdb_connection(db_path)
        try:
            # Verificar se tabela existe
            tables = con.execute("SHOW TABLES").fetchdf()
            table_exists = table_name in tables['name'].values if len(tables) > 0 else False
            
            if table_exists and if_exists == "fail":
                raise BronzeIngestionError(f"Tabela {table_name} já existe e if_exists='fail'")
            
            # Criar ou substituir tabela
            if if_exists == "replace":
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                logger.info(f"✅ Tabela {table_name} criada/substituída")
            elif if_exists == "append":
                if table_exists:
                    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                    logger.info(f"✅ Dados inseridos na tabela {table_name}")
                else:
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    logger.info(f"✅ Tabela {table_name} criada")
            else:
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
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
                "rows_ingested": len(df),
                "total_rows_in_table": int(total_rows),
                "columns": columns,
                "ingested_at": datetime.utcnow().isoformat(),
                "schema": schema_result.to_dict('records')
            }
            
            logger.info(f"📊 Ingestão concluída: {result['rows_ingested']} linhas -> {table_name}")
            logger.info(f"📋 Total na tabela: {result['total_rows_in_table']} linhas")
            
            return result
        finally:
            con.close()
            
    except Exception as e:
        error_msg = f"Erro ao criar tabela {table_name}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise BronzeIngestionError(error_msg)


def validate_json_field(
    df: pd.DataFrame,
    json_column: str,
    log_rejected: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Valida campo JSON em um DataFrame e separa linhas válidas e inválidas.
    
    Args:
        df: DataFrame com os dados
        json_column: Nome da coluna que contém JSON
        log_rejected: Se True, loga informações sobre linhas rejeitadas
        
    Returns:
        Tupla com (DataFrame válido, DataFrame rejeitado, estatísticas)
    """
    if json_column not in df.columns:
        raise BronzeIngestionError(
            f"Coluna JSON '{json_column}' não encontrada. "
            f"Colunas disponíveis: {list(df.columns)}"
        )
    
    logger.info(f"🔍 Validando campo JSON: {json_column}")
    
    valid_rows = []
    rejected_rows = []
    rejected_indices = []
    
    for idx, row in df.iterrows():
        json_value = row[json_column]
        
        # Verificar se é nulo
        if pd.isna(json_value):
            rejected_rows.append({
                'index': idx,
                'row_data': row.to_dict(),
                'reason': 'JSON field is NULL'
            })
            rejected_indices.append(idx)
            continue
        
        # Converter para string se necessário
        json_str = str(json_value).strip()
        
        # Verificar se está vazio
        if not json_str or json_str == '':
            rejected_rows.append({
                'index': idx,
                'row_data': row.to_dict(),
                'reason': 'JSON field is empty'
            })
            rejected_indices.append(idx)
            continue
        
        # Tentar parsear JSON
        try:
            json.loads(json_str)
            valid_rows.append(idx)
        except json.JSONDecodeError as e:
            rejected_rows.append({
                'index': idx,
                'row_data': row.to_dict(),
                'reason': f'Invalid JSON: {str(e)}',
                'json_value': json_str[:200]  # Primeiros 200 caracteres para log
            })
            rejected_indices.append(idx)
        except Exception as e:
            rejected_rows.append({
                'index': idx,
                'row_data': row.to_dict(),
                'reason': f'Unexpected error: {str(e)}',
                'json_value': json_str[:200]
            })
            rejected_indices.append(idx)
    
    # Separar DataFrames
    df_valid = df.loc[valid_rows].copy() if valid_rows else pd.DataFrame()
    df_rejected = df.loc[rejected_indices].copy() if rejected_indices else pd.DataFrame()
    
    # Estatísticas
    total_rows = len(df)
    valid_count = len(valid_rows)
    rejected_count = len(rejected_indices)
    
    stats = {
        'total_rows': total_rows,
        'valid_rows': valid_count,
        'rejected_rows': rejected_count,
        'rejection_rate': (rejected_count / total_rows * 100) if total_rows > 0 else 0
    }
    
    # Logging
    valid_rate = (valid_count / total_rows * 100) if total_rows > 0 else 0
    logger.info(f"📊 Validação JSON concluída:")
    logger.info(f"   ✅ Linhas válidas: {valid_count:,} ({valid_rate:.2f}% válidas)")
    logger.info(f"   ❌ Linhas rejeitadas: {rejected_count:,} ({stats['rejection_rate']:.2f}% rejeitadas)")
    
    if rejected_count > 0 and log_rejected:
        logger.warning(f"⚠️ QUALITY GATE: {rejected_count} linhas rejeitadas devido a JSON inválido")
        
        # Log detalhado das primeiras 10 linhas rejeitadas
        for i, rejected in enumerate(rejected_rows[:10]):
            logger.warning(
                f"   Rejeitada #{i+1} (linha {rejected['index']}): "
                f"{rejected['reason']}"
            )
            if 'json_value' in rejected:
                logger.warning(f"      JSON (primeiros 200 chars): {rejected['json_value']}")
        
        if rejected_count > 10:
            logger.warning(f"   ... e mais {rejected_count - 10} linhas rejeitadas")
    
    return df_valid, df_rejected, stats


def ingest_csv_to_bronze(
    file_path: str,
    table_name: str,
    db_path: str = DB_PATH,
    expected_columns: Optional[List[str]] = None,
    min_rows: int = 0,
    if_exists: str = "replace",
    add_metadata: bool = True,
    validate_json: Optional[str] = None,
    **csv_kwargs
) -> Dict[str, Any]:
    """
    Função completa de ingestão CSV para bronze.
    Combina validação, leitura e criação de tabela.
    
    Args:
        file_path: Caminho do arquivo CSV
        table_name: Nome da tabela bronze
        db_path: Caminho do arquivo DuckDB
        expected_columns: Colunas esperadas (opcional)
        min_rows: Número mínimo de linhas
        if_exists: Comportamento se tabela existir
        add_metadata: Adicionar colunas de metadados
        validate_json: Nome da coluna JSON para validar (opcional)
        **csv_kwargs: Argumentos adicionais para pd.read_csv
        
    Returns:
        Dicionário com informações da ingestão
    """
    logger.info(f"🚀 Iniciando ingestão bronze: {file_path} -> {table_name}")
    
    # 1. Validar arquivo
    validate_file_exists(file_path)
    
    # 2. Ler CSV com validações
    df = read_csv_with_validation(
        file_path,
        expected_columns=expected_columns,
        min_rows=min_rows,
        **csv_kwargs
    )
    
    # 3. Validar JSON se solicitado (QUALITY GATE)
    json_stats = None
    if validate_json:
        df_valid, df_rejected, json_stats = validate_json_field(
            df,
            json_column=validate_json,
            log_rejected=True
        )
        
        if len(df_valid) == 0:
            raise BronzeIngestionError(
                f"QUALITY GATE FALHOU: Todas as {len(df)} linhas foram rejeitadas "
                f"devido a JSON inválido na coluna '{validate_json}'. "
                f"Nenhuma linha será inserida no banco."
            )
        
        # Usar apenas linhas válidas
        df = df_valid
        logger.info(f"✅ QUALITY GATE: {len(df)} linhas válidas serão inseridas")
    
    # 4. Criar tabela bronze
    result = create_bronze_table(
        df,
        table_name,
        db_path=db_path,
        if_exists=if_exists,
        add_metadata=add_metadata
    )
    
    # Adicionar estatísticas de validação JSON ao resultado
    if json_stats:
        result['json_validation'] = json_stats
        result['rejected_rows_count'] = json_stats['rejected_rows']
    
    logger.info(f"✅ Ingestão bronze concluída com sucesso!")
    return result

