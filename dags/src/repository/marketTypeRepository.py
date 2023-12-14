def insert_table_market_type(conn):
    insert = '''
    INSERT INTO dim_tipo_mercado (
                SELECT DISTINCT tipo_mercado,
                    CASE
                        WHEN tipo_mercado = '10' THEN 'VISTA'
                        WHEN tipo_mercado = '12' THEN 'EXERCICIO DE OPCOESES DE COMPRA'
                        WHEN tipo_mercado = '13' THEN 'EXERCICIO DE OPCOES DE VENDA'
                        WHEN tipo_mercado = '17' THEN 'LEILAO'
                        WHEN tipo_mercado = '20' THEN 'FRACIONARIO'
                        WHEN tipo_mercado = '30' THEN 'TERMO'
                        WHEN tipo_mercado = '50' THEN 'FUTURO COM RETENCAO DE GANHO'
                        WHEN tipo_mercado = '60' THEN 'FUTURO COM MOVIMENTACAO CONTINUA'
                        WHEN tipo_mercado = '70' THEN 'OPCOES DE COMPRA'
                        WHEN tipo_mercado = '80' THEN 'OPCOES DE VENDA'
                        ELSE NULL
                    END AS desc_tipo_mercado
                FROM btres
            )
    '''
    conn.cursor().execute(insert)
    conn.commit()
    

def create_table_market_type(conn):
    delete = ''' 
            DROP TABLE IF EXISTS dim_tipo_mercado CASCADE; 
        '''
    conn.cursor().execute(delete)
    conn.commit()
    
    create = '''
            CREATE TABLE dim_tipo_mercado (
                tipo_mercado bigint PRIMARY KEY, 
                desc_tipo_mercado varchar(255)
            )
        '''
    conn.cursor().execute(create)
    conn.commit()