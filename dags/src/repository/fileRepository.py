from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_enginer():
     return create_engine('postgresql+psycopg2://airflow:airflow@host.docker.internal:5432/airflow')

def connectBdd():

    hook = PostgresHook(postgres_conn_id='teste')
    conn = hook.get_conn()
    return conn
    
def drop_table(conn): 
    drop ='''
    DROP TABLE IF EXISTS btres
    '''

    conn.cursor().execute(drop)
    conn.commit()
    conn.cursor().close()
    
def create_table(conn):

    create = '''
    CREATE TABLE IF NOT EXISTS btres (
        id_pregao bigserial primary key
        , tipo_registro bigint
        , data_pregao date
        , cod_bdi bigint
        , cod_negociacao varchar(255)
        , tipo_mercado bigint
        , nome_empresa varchar(255)
        , especificacao_papel varchar(255)
        , prazo_dias_merc_termo varchar(255)
        , moeda_referencia varchar(255)
        , preco_abertura decimal
        , preco_maximo decimal
        , preco_minimo decimal
        , preco_medio decimal
        , preco_ultimo_negocio decimal
        , preco_melhor_oferta_compra decimal
        , preco_melhor_oferta_venda decimal
        , numero_negocios bigint
        , quantidade_papeis_negociados bigint
        , volume_total_negociado bigint
        , preco_exercicio decimal
        , indicador_correcao_precos bigint
        , data_vencimento varchar(255)
        , fator_cotacao bigint
        , preco_exercicio_pontos bigint,
        codigo_isin VARCHAR(255)
        , num_distribuicao_papel bigint
    );
    '''
    conn.cursor().execute(create)
    conn.commit()
    conn.cursor().close()
    
