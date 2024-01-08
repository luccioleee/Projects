sql_list = {
    "getFiliaisList": '''
        SELECT DISTINCT 
            n.filial AS numfilial
        FROM movimento m
        INNER JOIN nf n 
            ON n.TIPO_OPERACAO = m.TIPO_OPERACAO 
            AND n.COD_OPERACAO = m.COD_OPERACAO
        where 
            n.TIPO_OPERACAO = 'E' 
    ''',
    "get_idworksxmillennium" : ''' 
        SELECT *
        FROM nfo_idworksxmillennium
    ''',
    "erase_previoustable" :
    '''
        DROP TABLE IF EXISTS dbo.{nometabela};
    ''',

    "get_saidas": '''
        select 
            cb.BARRA,
            p.COD_PRODUTO, 
            pe.produto, 
            c.COD_COR, 
            pe.tamanho, 
            sum(pe.quantidade) as quantidade 
        from movimento m
        inner join produtos_eventos pe on m.TIPO_OPERACAO = pe.TIPO_OPERACAO and pe.COD_OPERACAO = m.COD_OPERACAO
        inner join nf n on n.TIPO_OPERACAO = m.TIPO_OPERACAO and n.COD_OPERACAO = m.COD_OPERACAO
        inner join produtos p on p.produto = pe.PRODUTO
        inner join cores c on c.cor = pe.cor
        inner join codigo_barras cb on cb.PRODUTO = pe.PRODUTO and cb.COR = pe.cor and cb.ESTAMPA = pe.ESTAMPA and cb.TAMANHO = pe.TAMANHO and cb.TIPO_BARRA = 9
        where n.TIPO_OPERACAO = 'S' 
        ANd n.filial = {numerofilial}
        and cb.BARRA = '{codbarra}'
        group by cb.BARRA,p.COD_PRODUTO, pe.produto,c.COD_COR, pe.tamanho
    ''',
    "get_entradas_by_numero": '''
        select 
            m.data AS data_movimento, 
            n.data AS data_nf, 
            n.nota AS nota,
            cb.BARRA AS codbarra,   
            p.COD_PRODUTO AS codproduto, 
            p.produto AS produto,
            c.COD_COR AS codcor, 
            pe.TAMANHO AS codtamanho, 
            p.descricao1 AS descricao,
            pe.QUANTIDADE AS quantidade,
            pe.PRECO AS preco,
            (pe.QUANTIDADE * pe.PRECO) AS valor_total,
            CAST(COALESCE((select top 1 CUSTO_MEDIO from SL_CUSTO_MEDIO where produto = p.produto AND DATA <= m.data AND TIPO = 'ENTRADA' order by ordem desc),0) AS float) AS custo_medio,
            f.COD_FILIAL AS codfilial,
            n.FILIAL AS filial 
        from movimento m
        inner join produtos_eventos pe on m.TIPO_OPERACAO = pe.TIPO_OPERACAO and  pe.COD_OPERACAO = m.COD_OPERACAO
        inner join nf n on n.TIPO_OPERACAO = m.TIPO_OPERACAO and n.COD_OPERACAO = m.COD_OPERACAO
        inner join produtos p on p.produto = pe.PRODUTO
        inner join filiais f on f.filial = n.FILIAL
        inner join cores c on c.cor = pe.cor
        inner join codigo_barras cb on cb.PRODUTO = pe.PRODUTO and cb.COR = pe.cor and cb.ESTAMPA = pe.ESTAMPA and cb.TAMANHO = pe.TAMANHO and cb.TIPO_BARRA = 9
        inner join mov_estoque me on me.TIPO_ORIGEM = n.TIPO_OPERACAO and me.ORIGEM = n.COD_OPERACAO and pe.PRODuto = me.PRODUTO and pe.cor = me.cor and pe.ESTAMPA = pe.ESTAMPA and me.TAMANHO = pe.TAMANHO
        where n.TIPO_OPERACAO = 'E' 
        and n.filial = {numerofilial}
    '''


}
