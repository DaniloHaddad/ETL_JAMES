def create_table_paper(conn):
    delete = ''' 
            DROP TABLE IF EXISTS dim_papel CASCADE; 
        '''
    conn.cursor().execute(delete)
    conn.commit()
    
    create = '''
    CREATE TABLE dim_papel (
                especificacao_papel varchar(255) primary key, 
                num_distribuicao_papel bigint, 
                codigo_isin varchar(255) 
            )
    '''
    conn.cursor().execute(create)
    conn.commit()

    
def insert_table_paper(conn):
    insert = '''
    INSERT INTO dim_papel(especificacao_papel, num_distribuicao_papel, codigo_isin)
            SELECT DISTINCT especificacao_papel, num_distribuicao_papel, codigo_isin 
            FROM btres
            ON CONFLICT DO NOTHING
    '''

    conn.cursor().execute(insert)
    conn.commit()