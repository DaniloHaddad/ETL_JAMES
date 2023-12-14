def insert_table_bdi(conn):
    insert = '''
    INSERT INTO dim_cod_bdi (cod_bdi, descricao)
            SELECT DISTINCT cod_bdi,
                CASE
                    WHEN cod_bdi = '2' THEN 'LOTE PADRAO'
                    WHEN cod_bdi = '5' THEN 'SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA'
                    WHEN cod_bdi = '6' THEN 'CONCORDATARIAS'
                    WHEN cod_bdi = '7' THEN 'RECUPERACAO EXTRAJUDICIAL'
                    WHEN cod_bdi = '8' THEN 'RECUPERAÇÃO JUDICIAL'
                    WHEN cod_bdi = '9' THEN 'RAET - REGIME DE ADMINISTRACAO ESPECIAL TEMPORARIA'
                    WHEN cod_bdi = '10' THEN 'DIREITOS E RECIBOS'
                    WHEN cod_bdi = '11' THEN 'INTERVENCAO'
                    WHEN cod_bdi = '12' THEN 'FUNDOS IMOBILIARIOS'
                    WHEN cod_bdi = '14' THEN 'CERT.INVEST/TIT.DIV.PUBLICA'
                    WHEN cod_bdi = '18' THEN 'OBRIGACÕES'
                    WHEN cod_bdi = '22' THEN 'BÔNUS (PRIVADOS)'
                    WHEN cod_bdi = '26' THEN 'APOLICES/BÔNUS/TITULOS PUBLICOS'
                    WHEN cod_bdi = '32' THEN 'EXERCICIO DE OPCOES DE COMPRA DE INDICES'
                    WHEN cod_bdi = '33' THEN 'EXERCICIO DE OPCOES DE VENDA DE INDICES'
                    WHEN cod_bdi = '38' THEN 'EXERCICIO DE OPCOES DE COMPRA'
                    WHEN cod_bdi = '42' THEN 'EXERCICIO DE OPCOES DE VENDA'
                    WHEN cod_bdi = '46' THEN 'LEILAO DE NAO COTADOS'
                    WHEN cod_bdi = '48' THEN 'LEILAO DE PRIVATIZACAO'
                    WHEN cod_bdi = '49' THEN 'LEILAO DO FUNDO RECUPERACAO ECONOMICA ESPIRITO SANTO'
                    WHEN cod_bdi = '50' THEN 'LEILAO'
                    WHEN cod_bdi = '51' THEN 'LEILAO FINOR'
                    WHEN cod_bdi = '52' THEN 'LEILAO FINAM'
                    WHEN cod_bdi = '53' THEN 'LEILAO FISET'
                    WHEN cod_bdi = '54' THEN 'LEILAO DE ACÕES EM MORA'
                    WHEN cod_bdi = '56' THEN 'VENDAS POR ALVARA JUDICIAL'
                    WHEN cod_bdi = '58' THEN 'OUTROS'
                    WHEN cod_bdi = '60' THEN 'PERMUTA POR ACÕES'
                    WHEN cod_bdi = '61' THEN 'META'
                    WHEN cod_bdi = '62' THEN 'MERCADO A TERMO'
                    WHEN cod_bdi = '66' THEN 'DEBENTURES COM DATA DE VENCIMENTO ATE 3 ANOS'
                    WHEN cod_bdi = '68' THEN 'DEBENTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS'
                    WHEN cod_bdi = '70' THEN 'FUTURO COM RETENCAO DE GANHOS'
                    WHEN cod_bdi = '71' THEN 'MERCADO DE FUTURO'
                    WHEN cod_bdi = '74' THEN 'OPCOES DE COMPRA DE INDICES'
                    WHEN cod_bdi = '75' THEN 'OPCOES DE VENDA DE INDICES'
                    WHEN cod_bdi = '78' THEN 'OPCOES DE COMPRA'
                    WHEN cod_bdi = '82' THEN 'OPCOES DE VENDA'
                    WHEN cod_bdi = '83' THEN 'BOVESPAFIX'
                    WHEN cod_bdi = '84' THEN 'SOMA FIX'
                    WHEN cod_bdi = '90' THEN 'TERMO VISTA REGISTRADO'
                    WHEN cod_bdi = '96' THEN 'MERCADO FRACIONARIO'
                    WHEN cod_bdi = '99' THEN 'TOTAL GERAL'
                    ELSE NULL
                END AS descricao
            FROM btres
    '''
    conn.cursor().execute(insert)
    conn.commit()
    conn.cursor().close()

def create_table_bdi(conn):
    
    delete = ''' DROP TABLE IF EXISTS dim_cod_bdi CASCADE; '''
    
    conn.cursor().execute(delete)
    conn.commit()

    create = '''
            CREATE TABLE dim_cod_bdi (
                cod_bdi bigint primary key, 
                descricao varchar(255)
            )
        '''
    
    conn.cursor().execute(create)
    conn.commit()