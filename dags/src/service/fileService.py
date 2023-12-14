import pandas as pd

def fommaterb3(fileb3):   

    sizeFields = [2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]

    formattedB3 = pd.read_fwf(fileb3, widths=sizeFields, header=0)

    formattedB3.columns = ["tipo_registro","data_pregao","cod_bdi","cod_negociacao","tipo_mercado","nome_empresa","especificacao_papel","prazo_dias_merc_termo","moeda_referencia","preco_abertura","preco_maximo","preco_minimo","preco_medio","preco_ultimo_negocio","preco_melhor_oferta_compra","preco_melhor_oferta_venda","numero_negocios","quantidade_papeis_negociados","volume_total_negociado","preco_exercicio","indicador_correcao_precos","data_vencimento","fator_cotacao","preco_exercicio_pontos","codigo_isin","num_distribuicao_papel"]
    pd.set_option('display.max_columns', None)

    line = len(formattedB3["data_pregao"])
    formattedB3=formattedB3.drop(line-1)

    listaVirgula = ["preco_abertura","preco_maximo","preco_minimo","preco_medio","preco_ultimo_negocio","preco_melhor_oferta_compra","preco_melhor_oferta_venda","volume_total_negociado","preco_exercicio","preco_exercicio_pontos"]

    for coluna in listaVirgula:
        formattedB3[coluna] = [i/100. for i in formattedB3[coluna]]

    return formattedB3