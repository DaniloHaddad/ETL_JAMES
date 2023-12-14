def create_table_pregao(conn):
    delete = ''' 
            DROP TABLE IF EXISTS fato_pregao CASCADE; 
        '''
    conn.cursor().execute(delete)
    conn.commit()
    
    create = '''
        CREATE TABLE fato_pregao (
                    id_pregao bigint primary key,
                    cod_bdi bigint,
                    tipo_mercado bigint,
                    cod_negociacao varchar(255), 
                    especificacao_papel varchar(255),
                    numero bigint, 
                    data_pregao date,
                    preco_melhor_oferta_compra decimal, 
                    preco_melhor_oferta_venda decimal, 
                    moeda_referencia varchar(255), 
                    numero_negocios bigint, 
                    preco_abertura decimal,  
                    preco_maximo decimal, 
                    preco_medio decimal, 
                    preco_minimo decimal, 
                    preco_ultimo_negocio decimal,
                    tipo_registro bigint, 
                    volume_total_negociado bigint

                )
    '''
    conn.cursor().execute(create)
    conn.commit()

    
def insert_table_pregao(conn):
    insert = '''
    INSERT INTO fato_pregao(
                id_pregao, cod_bdi, tipo_mercado, cod_negociacao, especificacao_papel, numero, data_pregao, preco_melhor_oferta_compra, 
                preco_melhor_oferta_venda, moeda_referencia, numero_negocios, preco_abertura, preco_maximo, preco_medio, preco_minimo, 
                preco_ultimo_negocio, tipo_registro, volume_total_negociado
            )
            SELECT
                b.id_pregao,
                dc.cod_bdi, 
                dtm.tipo_mercado,
                de.cod_negociacao, 
                dp.especificacao_papel,
                cp.numero,
                b.data_pregao, 
                b.preco_melhor_oferta_compra, 
                b.preco_melhor_oferta_venda,
                b.moeda_referencia, 
                b.numero_negocios, 
                b.preco_abertura,  
                b.preco_maximo, 
                b.preco_medio, 
                b.preco_minimo,
                b.preco_ultimo_negocio, 
                b.tipo_registro, 
                b.volume_total_negociado 
            FROM btres b
            JOIN dim_cod_bdi dc ON b.cod_bdi = dc.cod_bdi
            JOIN dim_tipo_mercado dtm ON b.tipo_mercado = dtm.tipo_mercado
            JOIN dim_empresa de ON b.cod_negociacao = de.cod_negociacao
            JOIN dim_papel dp ON b.especificacao_papel = dp.especificacao_papel
            JOIN dim_correcao_preco cp ON b.indicador_correcao_precos = cp.numero
    '''

    conn.cursor().execute(insert)
    conn.commit()

def create_relation(conn):
    relations ='''
    ALTER TABLE IF EXISTS public.fato_pregao
        ADD CONSTRAINT fk_cod_bdi FOREIGN KEY (cod_bdi)
        REFERENCES public.dim_cod_bdi (cod_bdi) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

    ALTER TABLE IF EXISTS public.fato_pregao
        ADD CONSTRAINT fk_tipo_mercado FOREIGN KEY (tipo_mercado)
        REFERENCES public.dim_tipo_mercado (tipo_mercado) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

    ALTER TABLE IF EXISTS public.fato_pregao
        ADD CONSTRAINT fk_empresa FOREIGN KEY (cod_negociacao)
        REFERENCES public.dim_empresa (cod_negociacao) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

    ALTER TABLE IF EXISTS public.fato_pregao
        ADD CONSTRAINT fk_papel FOREIGN KEY (especificacao_papel)
        REFERENCES public.dim_papel (especificacao_papel) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

    ALTER TABLE IF EXISTS public.fato_pregao
        ADD CONSTRAINT fk_correcao_preco FOREIGN KEY (numero)
        REFERENCES public.dim_correcao_preco (numero) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
    '''

    conn.cursor().execute(relations)
    conn.commit()