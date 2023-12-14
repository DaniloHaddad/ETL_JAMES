def create_table_price_correction(conn):
    delete = ''' 
            DROP TABLE IF EXISTS dim_correcao_preco CASCADE; 
        '''
    conn.cursor().execute(delete)
    conn.commit()
    
    create = '''
    CREATE TABLE dim_correcao_preco (
            numero INT PRIMARY KEY,
            valor VARCHAR(100)
        );
    '''
    conn.cursor().execute(create)
    conn.commit()

    
def insert_table_price_correction(conn):
    insert = '''
    INSERT INTO dim_correcao_preco (numero, valor) VALUES
        (0, 'NEUTRO'),
        (1, 'US$ CORREÇÃO PELA TAXA DO DÓLAR'),
        (2, 'TJLP CORREÇÃO PELA TJLP'),
        (8, 'IGPM CORREÇÃO PELO IGP-M - OPÇÕES PROTEGIDAS'),
        (9, 'URV CORREÇÃO PELA URV');
    '''

    conn.cursor().execute(insert)
    conn.commit()