def create_table_company(conn):
    delete = ''' 
            DROP TABLE IF EXISTS dim_empresa CASCADE; 
        '''
    conn.cursor().execute(delete)
    conn.commit()
    
    create = '''
    CREATE TABLE dim_empresa (
                cod_negociacao varchar(255) primary key, 
                nome_empresa varchar(255) 
            ) 
    '''
    conn.cursor().execute(create)
    conn.commit()

    
def insert_table_company(conn):
    insert = '''
    INSERT INTO dim_empresa (cod_negociacao, nome_empresa)
            SELECT DISTINCT cod_negociacao, nome_empresa
            FROM btres
            ON CONFLICT DO NOTHING
    '''

    conn.cursor().execute(insert)
    conn.commit()