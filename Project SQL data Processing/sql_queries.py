sql_list = {
"getFiliaisList" : '''
    SELECT
        fm.COD_FILIAL as codFilialMill,
        odlff.nomeProcesso as nomeProcesso,
        odlff.numFilial as numFilial,
        fm.FILIAL as nomeFilialMill
    FROM 
        OneD_lFechamentoFiliais odlff
    LEFT JOIN	
        filiais_millennium fm 
        ON
        odlff.numFilial = fm.NUM_FILIAL 
    GROUP BY
        odlff.numFilial,
        fm.COD_FILIAL,
        odlff.nomeProcesso,
        fm.FILIAL
''',
"get_filiaisList_datapipeline_fRelFechamento" : '''
    SELECT 
        *
    FROM
        OneD_lFechamentoFiliais
''',
"getGMV" : '''
    SELECT
        *
    FROM
        OneD_regrasGMVFechamento 
    WHERE
        client = '%s'
''',
"getOthers" : '''
    SELECT
        *
    FROM
        OneD_regrasOtherFechamento
    WHERE
        client = '%s'
''',

############################################## - ARMAZENAMENTO - ######################################################
#atualizado - imposto ok - datas ok
"ARMZ_LCPDR" :'''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
        FROM
        (
            SELECT 
                GETDATE() as dataconsulta,
                DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
                DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
                tabela.datainclusao as datapico,
                tabela.filial as filial,
                regrasFechamento.numFilial AS NUM_FILIAL,
                regrasFechamento.mainrule as regra,
                regrasFechamento.subrule as subregra,
                regrasFechamento.item as descricao,
                tabela.quantidade as quantidade,
                NULL as valortotal,
                regrasFechamento.price as preco,
                regrasFechamento.unit as unidade,
                tabela.quantidade * COALESCE(regrasFechamento.price, 0) AS valorfinal
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY Significado, filial ORDER BY quantidade DESC) AS num_linha
                FROM (
                    SELECT
                        wvsqpds.datainclusao,
                        wvsqpds.filial,
                        SUM(wvsqpds.qtde) AS quantidade,
                        odna.significado 
                    FROM
                        wms_vSpQtdePosicaoDiarioSuccess wvsqpds 
                    LEFT JOIN
                        OneD_nomenclaturaARMZFechamento odna ON LEFT(wvsqpds.siglarua, 2) = odna.Sigla
                    WHERE
                        wvsqpds.datainclusao BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                    GROUP BY
                        odna.significado,
                        wvsqpds.datainclusao,
                        wvsqpds.filial
                ) as resultado
            ) as tabela
            LEFT JOIN (
                SELECT 
                    *
                FROM 
                    OneD_regrasOtherFechamento odrof
                LEFT JOIN
                    (
                        SELECT
                            av.CONTA AS nomeVTEX,
                            odlff.numFilial,
                            odlff.nomeProcesso,
                            odlff.codFilialMill
                        FROM
                            OneD_lFechamentoFiliais odlff 
                        LEFT JOIN
                            acessos_vtex av 
                            ON
                            odlff.nomeFilialVtex = av.NOME_FILIAL 
                    ) resultado1
                    ON
                    resultado1.nomeProcesso = odrof.client
                WHERE
                    odrof.subrule IN ('LC_PDR')
                AND 
                    odrof.mainrule = 'ARMZ'
                ) AS regrasFechamento ON regrasFechamento.item = tabela.significado
                                    AND regrasFechamento.codFilialMill = tabela.filial
            WHERE
                tabela.num_linha = 1
                AND
                regrasFechamento.numFilial = {numerofilial}
        ) resultado6
        LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'ARMZ'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
	''',

#atualizado - imposto ok - datas ok
"ARMZ_SGPDR" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT 
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            tabela.datainclusao as datapico,
            tabela.filial as filial,
            regrasFechamento.numFilial as NUM_FILIAL,
            regrasFechamento.mainrule as regra,
            regrasFechamento.subrule as subregra,
            regrasFechamento.item as descricao,
            NULL as quantidade,
            tabela.valorestoque as valorTotal,
            regrasFechamento.price as preco,
            regrasFechamento.unit as unidade,
            tabela.valorestoque * COALESCE(regrasFechamento.price, 0)/ 100 AS valorfinal
        FROM (
            SELECT
                *,
                'Seguro' as significado,
                ROW_NUMBER() OVER (PARTITION BY filial ORDER BY valorestoque DESC) AS num_linha
            FROM (
                SELECT
                    wvsseds.datainclusao,
                    wvsseds.filial,
                    wvsseds.valorestoque
                FROM
                    wms_vSpSaldoEstoqueDiarioSuccess wvsseds  
                WHERE
                    wvsseds.datainclusao BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                            AND  DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                GROUP BY
                    wvsseds.datainclusao,
                    wvsseds.filial,
                    wvsseds.valorestoque
            ) as resultado
        ) as tabela
        LEFT JOIN (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso,
                        odlff.codFilialMill
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule IN ('SG_PDR')
            AND 
                odrof.mainrule = 'ARMZ'
        ) AS regrasFechamento ON regrasFechamento.item = tabela.significado
                            AND regrasFechamento.codFilialMill = tabela.filial
        WHERE
            tabela.num_linha = 1
            AND 
            regrasFechamento.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'ARMZ'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - MOVIMENTAÇÃO - ##################################################
#aguarda validação < usando mesmo sem validar - #atualizado - imposto ok - datas ok
"MOV_PDR" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            regrasFechamento.client as filial,
            regrasFechamento.numFilial as NUM_FILIAL,
            regrasFechamento.mainrule as regra,
            regrasFechamento.subrule as subregra,
            regrasFechamento.item + ' - ' + tabela.categoria_peso as descricao,
            tabela.valor as quantidade,
            NULL as valortotal,
            regrasFechamento.price as preco,
            regrasFechamento.unit as unidade,
            tabela.valor * COALESCE(regrasFechamento.price, 0) AS valorfinal
        FROM (
            SELECT
                SUM(ABS(resultado2.valor)) AS valor,
                resultado2.categoria_peso,
                resultado2.numFilial,
                resultado2.item
            FROM
            (
                SELECT 
                    valor,
                    resultado.categoria_peso,
                    resultado.numFilial,
                    item,
                    resultado.nota
                FROM (
                    SELECT
                        mfrfs_nm_cj_odrmt.recebimento AS recebimento,
                        mfrfs_nm_cj_odrmt.expedicao AS expedicao,
                        mfrfs_nm_cj_odrmt.devolucao AS devolucao,
                        mfrfs_nm_cj_odrmt.categoria_peso,
                        mfrfs_nm_cj_odrmt.numFilial,
                        mfrfs_nm_cj_odrmt.nota
                    FROM (
	                    SELECT
							mfrfs.*
						FROM 
							mill_fRelFechamentoSucess mfrfs 
						WHERE
							mfrfs.numFilial = {numerofilial}
					) mfrfs_nm_cj_odrmt        
                ) resultado
                CROSS apply (
                    VALUES 
                        ('recebimento', recebimento),
                        ('expedicao', expedicao),
                        ('devolucao', devolucao)
                ) c (item, valor)
            ) resultado2
            GROUP BY
                resultado2.categoria_peso,
                resultado2.numFilial,
                resultado2.item
        ) as tabela
        LEFT JOIN (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule IN ('MOV_PDR')
            AND 
                odrof.mainrule = 'MOV'
        ) AS regrasFechamento ON UPPER(regrasFechamento.item) = Upper(tabela.item)
                            AND UPPER(regrasFechamento.criteria) = UPPER(tabela.categoria_peso)
                            AND regrasFechamento.numFilial = tabela.numFilial
        WHERE 
            regrasFechamento.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'MOV'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

#atualizado - imposto ok - datas ok
"MOV_SC" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            regrasFechamento.client as filial,
            regrasFechamento.numFilial as NUM_FILIAL,
            regrasFechamento.mainrule as regra,
            regrasFechamento.subrule as subregra,
            --regrasFechamento.item + ' - ' + tabela.categoria_peso as descricao,
            regrasFechamento.item as descricao,
            tabela.valor as quantidade,
            NULL as valortotal,
            regrasFechamento.price as preco,
            regrasFechamento.unit as unidade,
            tabela.valor * COALESCE(regrasFechamento.price, 0) AS valorfinal
        FROM (
            SELECT
                SUM(ABS(resultado2.valor)) AS valor,
                --resultado2.categoria_peso,
                resultado2.numFilial,
                resultado2.item
            FROM
            (
                SELECT 
                    valor,
                    --resultado.categoria_peso,
                    resultado.numFilial,
                    item,
                    resultado.nota
                FROM (
                    SELECT
                        mfrfs_nm_cj_odrmt.recebimento AS recebimento,
                        mfrfs_nm_cj_odrmt.expedicao AS expedicao,
                        mfrfs_nm_cj_odrmt.devolucao AS devolucao,
                        --mfrfs_nm_cj_odrmt.categoria_peso,
                        mfrfs_nm_cj_odrmt.numFilial,
                        mfrfs_nm_cj_odrmt.nota
                    FROM (
                        SELECT
                            mfrfs.*
                        FROM 
                            mill_fRelFechamentoSucess mfrfs 
                        WHERE
                            mfrfs.numFilial = {numerofilial}
                    ) mfrfs_nm_cj_odrmt        
                ) resultado
                CROSS apply (
                    VALUES 
                        ('recebimento', recebimento),
                        ('expedicao', expedicao),
                        ('devolucao', devolucao)
                ) c (item, valor)
            ) resultado2
            GROUP BY
                --resultado2.categoria_peso,
                resultado2.numFilial,
                resultado2.item
        ) as tabela
        LEFT JOIN (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule IN ('MOV_SC')
            AND 
                odrof.mainrule = 'MOV'
        ) AS regrasFechamento ON UPPER(regrasFechamento.item) = Upper(tabela.item)
                            AND regrasFechamento.numFilial = tabela.numFilial
        WHERE 
            regrasFechamento.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'MOV'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - FRETE - #########################################################
#atualizado - imposto ok - datas ok
"FRT_CTE" : '''
    SELECT
        resultado6.dataconsulta,
        resultado6.dataperiodoinicio,
        resultado6.dataperiodofim,
        resultado6.datapico,
        resultado6.filial,
        resultado6.NUM_FILIAL,
        resultado6.regra,
        resultado6.subregra,
        resultado6.descricao,
        resultado6.quantidade,
        SUM(resultado6.valortotal) AS valortotal,
        --resultado6.preco AS preco,
        100 AS preco,
        resultado6.unidade,
        SUM(resultado6.valortotal) AS valorfinal,
        resultado6.preco AS markup,
        '%' AS unidademarkup,
        COALESCE(resultado6.preco/100,0) * SUM(resultado6.valortotal) AS valormarkup,
        SUM(resultado6.valorfinal) AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        SUM(resultado6.valorfinal) * (resultadofrete2.price/100) AS valorimposto,
        SUM(resultado6.valorfinal) * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,            
            NULL as datapico,
            resultado.client as filial,
            nm.NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            CASE	
                WHEN odrmt.aplicaMarkup = 'N' THEN resultado.item + ' markup N'
                WHEN odrmt.aplicaMarkup = 'S' THEN resultado.item + ' markup S'
                ELSE resultado.item + ' markup vazio'
            END AS descricao,
            NULL as quantidade,
            SUM(cj.VALOR_PRESTACAO) AS valortotal,
            CASE	
                WHEN odrmt.aplicaMarkup = 'N' THEN 0
                WHEN odrmt.aplicaMarkup = 'S' THEN resultado.price
                ELSE 0
            END AS preco,
            '%' as unidade,
            ((CASE        	
                WHEN odrmt.aplicaMarkup = 'N' THEN 100
                WHEN odrmt.aplicaMarkup = 'S' THEN 100 + resultado.price
                ELSE 100
                END)/100 * COALESCE(SUM(cj.VALOR_PRESTACAO), 0)) AS valorfinal
        FROM 
            ctes_jettax cj
        LEFT JOIN
            nfs_millennium nm 
            ON 
            cj.CHAVE_NFE = nm.CHAVE_NFE
        LEFT JOIN
            (
            SELECT 
                odrmt.aplicaMarkup,
                odrmt.CNPJTransportadora 
            FROM
                OneD_regraMarkupTransportadoras odrmt 
            ) odrmt
            ON
            cj.CNPJ_EMITENTE = odrmt.CNPJTransportadora 
        LEFT JOIN
            (
                SELECT 
                    odrof.client,
                    odrof.mainrule,
                    odrof.subrule,
                    odrof.item,
                    odrmf.markup_transportadora AS price,
                    '%' AS unit,
                    odrof.criteria,
                    odrof.obs,
                    resultado1.nomeVTEX,
                    resultado1.numFilial,
                    resultado1.nomeProcesso
                FROM 
                    OneD_regrasOtherFechamento odrof
                LEFT JOIN
                    (
                        SELECT
                            av.CONTA AS nomeVTEX,
                            odlff.numFilial,
                            odlff.nomeProcesso 
                        FROM
                            OneD_lFechamentoFiliais odlff 
                        LEFT JOIN
                            acessos_vtex av 
                            ON
                            odlff.nomeFilialVtex = av.NOME_FILIAL 
                    ) resultado1
                    ON
                    resultado1.nomeProcesso = odrof.client
                LEFT JOIN
                    (
                        SELECT
                            ROUND(REPLACE(odrmf.markup_transportadora, ',', '.'),4) AS markup_transportadora ,
                            odrmf.num_filial_millennium
                        FROM
                            OneD_regraMarkupFRTClientes odrmf
                    ) odrmf
                    ON
                    odrmf.num_filial_millennium = resultado1.numFilial
                WHERE
                    odrof.subrule = 'FRT_CTE'
                    AND 
                    odrof.mainrule = 'FRETE'
            ) resultado
            ON
            resultado.numFilial = nm.NUM_FILIAL 
        WHERE	
            nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                        AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
            AND 
            nm.NUM_FILIAL = {numerofilial}
        GROUP BY
            nm.NUM_FILIAL,
            nm.FILIAL,
            resultado.price,
            resultado.unit,
            resultado.client,
            resultado.mainrule,
            resultado.subrule,
            resultado.item,
            odrmt.aplicaMarkup,
            odrmt.CNPJTransportadora
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'FRETE'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
    GROUP BY
        resultado6.dataconsulta,
        resultado6.dataperiodoinicio,
        resultado6.dataperiodofim,
        resultado6.datapico,
        resultado6.filial,
        resultado6.NUM_FILIAL,
        resultado6.regra,
        resultado6.subregra,
        resultado6.descricao,
        resultado6.quantidade,
        resultado6.preco,
        resultado6.unidade,
        resultadofrete2.price,
        resultadofrete2.unit
''',

"FRT_COR" : '''
    SELECT
        resultado6.*,
        markupcorreios.markup_correios AS markup,
        '%' AS unidademarkup,
        resultado6.valorfinal * (COALESCE(CAST(markupcorreios.markup_correios AS FLOAT),0)/100) AS valormarkup,
        resultado6.valorfinal * (1 + COALESCE(CAST(markupcorreios.markup_correios AS FLOAT),0)/100) AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (1 + COALESCE(CAST(markupcorreios.markup_correios AS FLOAT),0)/100)  * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + COALESCE(CAST(markupcorreios.markup_correios AS FLOAT),0)/100)  * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,            
            NULL as datapico,
            resultado.client as filial,
            empmofctotalizado.NUM_FILIAL AS NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            NULL as quantidade,
            empmofctotalizado.valorcorreios_total AS valortotal,
            --resultado.price as preco,
            --resultado.unit as unidade,
            100 as preco,
            '%' as unidade,
            empmofctotalizado.valorcorreios_total AS valorfinal
        FROM 
            (
                SELECT
                    SUM(empmofc.valorcorreios) AS valorcorreios_total,
                    empmofc.NUM_FILIAL
                FROM
                    (
                        SELECT 
                            CAST(REPLACE(REPLACE(RIGHT(ofc.[Valor Liquido (R$)], LEN(ofc.[Valor Liquido (R$)]) -3), '.' , ''), ',', '.') AS float) AS valorcorreios,
                            empm.NUM_FILIAL AS NUM_FILIAL
                        FROM
                            OneD_faturaCorreios ofc
                        LEFT JOIN
                            (
                                SELECT
                                    pm.NUM_FILIAL,
                                    em.PEDIDOV,
                                    em.NUMERO_OBJETO 
                                FROM
                                    embarques_millennium em
                                LEFT JOIN
                                    pedidos_millennium pm 
                                    ON
                                    pm.PEDIDOV = em.PEDIDOV
                            ) empm
                            ON
                            empm.NUMERO_OBJETO = ofc.[Numero da Etiqueta]
                        WHERE
                            ofc.[Data da Postagem] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                                    AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                        AND 
                            empm.NUM_FILIAL = {numerofilial}
                ) AS empmofc
                GROUP BY
                    NUM_FILIAL	
        ) AS empmofctotalizado
        LEFT JOIN
            (
                SELECT 
                    *
                FROM 
                    OneD_regrasOtherFechamento odrof
                LEFT JOIN
                    (
                        SELECT
                            av.CONTA AS nomeVTEX,
                            odlff.numFilial,
                            odlff.nomeProcesso 
                        FROM
                            OneD_lFechamentoFiliais odlff 
                        LEFT JOIN
                            acessos_vtex av 
                            ON
                            odlff.nomeFilialVtex = av.NOME_FILIAL 
                    ) resultado1
                    ON
                    resultado1.nomeProcesso = odrof.client
                WHERE
                    odrof.subrule = 'FRT_COR'
                    AND 
                    odrof.mainrule = 'FRETE'
            ) resultado
            ON
            resultado.numFilial = empmofctotalizado.NUM_FILIAL     
        ) resultado6
    LEFT JOIN
        (
            SELECT
                odrmf.markup_correios AS markup_correios,
                odrmf.num_filial_millennium AS NUM_FILIAL 
            FROM
                OneD_regraMarkupFRTClientes odrmf 
        ) markupcorreios
        ON
        markupcorreios.NUM_FILIAL = resultado6.NUM_FILIAL
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'FRETE'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - GMV - ###########################################################
#ok - datas ok
"GMV_VNF_SP" : '''
    SELECT
        GETDATE() as dataconsulta,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
        NULL as datapico,
        resultado4.nomeProcesso as filial,
        resultado4.numFilial as NUM_FILIAL,
        resultado4.mainidentifier as regra,
        resultado4.subidentifier as subregra,
        resultado4.descricao as descricao,
        NULL as quantidade,
        SUM(resultado4.VALOR_NOTA) AS valortotal,
        resultado4.takeRate as preco,
        'R$' as unidade,
        SUM(COALESCE(resultado4.VALOR_NOTA,0) * COALESCE(resultado4.takeRate, 0)) AS valorfinal,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        SUM(COALESCE(resultado4.VALOR_NOTA,0) * COALESCE(resultado4.takeRate, 0)) AS valorcommarkup,
        0 AS imposto,
        '%' AS unidadeimposto,
        0 AS valorimposto,
        SUM(COALESCE(resultado4.VALOR_NOTA,0) * COALESCE(resultado4.takeRate, 0)) AS valorfinalcomimposto
    FROM (
        SELECT
            *
        FROM (
            SELECT
                nm.NOTA,
                nm.NUM_FILIAL,
                pm.N_PEDIDO_CLIENTE AS ORDERID,
                CASE 
                    WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                    WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                    WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                    WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                    ELSE 'SP'
                END AS BASE_CALCULO,
                nm.VALOR_NOTA, 
                pm.QUANTIDADE,
                nm.EVENTO,  
                nm.VALOR_NOTA AS TICKET_MEDIO
            FROM
                nfs_millennium nm
            LEFT JOIN pedidos_millennium pm ON nm.PEDIDOV = pm.PEDIDOV
            WHERE	
                nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                               AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                AND
                nm.PEDIDOV IS NOT NULL
                AND 
                nm.NUM_FILIAL = {numerofilial}
            ) resultado
        INNER JOIN (
            SELECT 
                *
            FROM
                OneD_regrasGMVFechamento odrg
            INNER JOIN (
                SELECT
                    *
                FROM
                    OneD_lFechamentoFiliais odlff
                ) resultado2 ON resultado2.nomeProcesso = odrg.client 
            ) resultado3 ON resultado.BASE_CALCULO = resultado3.subidentifier
                        AND resultado.VALOR_NOTA > resultado3.lowerRange 
                        AND resultado3.lowerRange > resultado3.upperRange
                        AND resultado3.mainidentifier = 'VNF'
                        AND resultado3.subidentifier = 'SP'
        UNION 
        SELECT
            *
        FROM (
            SELECT
                nm.NOTA,
                nm.NUM_FILIAL,
                pm.N_PEDIDO_CLIENTE AS ORDERID,
                CASE 
                    WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                    WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                    WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                    WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                    ELSE 'SP'
                END AS BASE_CALCULO,
                nm.VALOR_NOTA,
                pm.QUANTIDADE,
                nm.EVENTO,
                nm.VALOR_NOTA AS TICKET_MEDIO
            FROM
                nfs_millennium nm
            LEFT JOIN pedidos_millennium pm ON nm.PEDIDOV = pm.PEDIDOV
            WHERE	
                nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                               AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                AND
                nm.PEDIDOV IS NOT NULL
                AND 
                nm.NUM_FILIAL = {numerofilial}
            ) resultado
        INNER JOIN (
            SELECT 
                *
            FROM
                OneD_regrasGMVFechamento odrg
            INNER JOIN (
                SELECT
                    *
                FROM
                    OneD_lFechamentoFiliais odlff
                ) resultado2 ON resultado2.nomeProcesso = odrg.client 
            ) resultado3 ON resultado.BASE_CALCULO = resultado3.subidentifier
                        AND resultado.VALOR_NOTA BETWEEN resultado3.lowerRange AND resultado3.upperRange
                        AND resultado3.mainidentifier = 'VNF'
                        AND resultado3.subidentifier = 'SP'
    ) as resultado4
    WHERE
        resultado4.numFilial = {numerofilial}
    GROUP BY 
        resultado4.nomeProcesso,
        resultado4.numFilial,
        resultado4.mainrule,
        resultado4.mainidentifier,
        resultado4.subidentifier,
        resultado4.takeRate,
        resultado4.descricao
''',
#ok - datas ok - mais atualizado
"GMV_TM_SP" :'''
    WITH 
        cte_nmpm AS (
            SELECT
                nm.*,
                CASE 
                    WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                    WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                    WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                    WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                    ELSE 'SP'
                END AS BASE_CALCULO,
                pm.N_PEDIDO_CLIENTE,
                pm.QUANTIDADE,
                pm.STATUS_WORKFLOW 
            FROM 
                nfs_millennium nm
            LEFT JOIN
                pedidos_millennium pm ON pm.PEDIDOV = nm.PEDIDOV
            WHERE
                nm.PEDIDOV IS NOT NULL
                AND nm.CANCELADA = 'F'
                AND nm.TIPO_FATURAMENTO <> 'Outro'
                AND nm.NUM_FILIAL = {numerofilial}
                AND nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},23,59,0,0)
        ),
        cte_odrg AS (
            SELECT 
                *
            FROM
                OneD_regrasGMVFechamento odrg_2
            LEFT JOIN (
                SELECT
                    *
                FROM
                    OneD_lFechamentoFiliais odlff_2
            ) resultado2_2 ON resultado2_2.nomeProcesso = odrg_2.client 
            WHERE
                    odrg_2.mainidentifier = 'TM'
                AND odrg_2.subidentifier = 'SP'
        )
    SELECT
        GETDATE() as dataconsulta,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},23,59,0,0) as dataperiodofim,
        NULL as datapico,
        resultadofinal.nomeProcesso as filial,
        resultadofinal.numFilial as NUM_FILIAL,
        resultadofinal.mainidentifier as regra,
        resultadofinal.subidentifier as subregra,
        resultadofinal.descricao as descricao,
        resultadofinal.NOTAS_TOTAIS as quantidade,
        resultadofinal.VALOR_TOTAL AS valortotal,
        resultadofinal.takeRate as preco,
        'R$' as unidade,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinal,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorcommarkup,
        0 AS imposto,
        '%' AS unidadeimposto,
        0 AS valorimposto,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinalcomimposto
    FROM (
        SELECT 
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                (SELECT
                    SUM(cte_nmpm.VALOR_NOTA) AS valorfaturado
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.STATUS_WORKFLOW IN ('AG. EMBARQUE', 'AG. ENTREGA', 'AG. TRANSPORTADORA', 'ENTREGUE', 'EXTRAVIADO', 'FINALIZADO', 'INSUCESSO', 'EM DEVOLUÇÃO')   	        
                ) 
                / 
                (SELECT
                    COUNT(cte_nmpm.PEDIDOV) AS pedidosfaturados
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.FATURADO = 'T'
                    AND cte_nmpm.TIPO_FATURAMENTO = 'Faturamento'
                ) AS TICKET_MEDIO,
                BASE_CALCULO
            FROM
                cte_nmpm
            GROUP BY
                BASE_CALCULO
        ) resumo2
        INNER JOIN cte_odrg ON resumo2.BASE_CALCULO = cte_odrg.subidentifier
                        AND resumo2.TICKET_MEDIO > cte_odrg.lowerRange 
                        AND cte_odrg.lowerRange > cte_odrg.upperRange
        UNION
        SELECT 
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                (SELECT
                    SUM(cte_nmpm.VALOR_NOTA) AS valorfaturado
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.STATUS_WORKFLOW IN ('AG. EMBARQUE', 'AG. ENTREGA', 'AG. TRANSPORTADORA', 'ENTREGUE', 'EXTRAVIADO', 'FINALIZADO', 'INSUCESSO', 'EM DEVOLUÇÃO')   	        
                ) 
                / 
                (SELECT
                    COUNT(cte_nmpm.PEDIDOV) AS pedidosfaturados
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.FATURADO = 'T'
                    AND cte_nmpm.TIPO_FATURAMENTO = 'Faturamento'
                ) AS TICKET_MEDIO,
                BASE_CALCULO
            FROM
                cte_nmpm
            GROUP BY
                BASE_CALCULO
        ) resumo2
        INNER JOIN cte_odrg ON resumo2.BASE_CALCULO = cte_odrg.subidentifier
                        AND resumo2.TICKET_MEDIO BETWEEN cte_odrg.lowerRange AND cte_odrg.upperRange
    ) resultadofinal
    GROUP BY
        resultadofinal.nomeProcesso,
        resultadofinal.mainrule,
        resultadofinal.numFilial,
        resultadofinal.mainidentifier,
        resultadofinal.subidentifier,
        resultadofinal.descricao,
        resultadofinal.NOTAS_TOTAIS,
        resultadofinal.TICKET_MEDIO,
        resultadofinal.VALOR_TOTAL,
        resultadofinal.takeRate
''',
#ok - datas ok- mais atualizado
"GMV_TM_MP" :'''
    WITH 
        cte_nmpm AS (
            SELECT
                nm.*,
                CASE 
                    WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                    WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                    WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                    WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                    ELSE 'SP'
                END AS BASE_CALCULO,
                pm.N_PEDIDO_CLIENTE,
                pm.QUANTIDADE,
                pm.STATUS_WORKFLOW 
            FROM 
                nfs_millennium nm
            LEFT JOIN
                pedidos_millennium pm ON pm.PEDIDOV = nm.PEDIDOV
            WHERE
                nm.PEDIDOV IS NOT NULL
                AND nm.CANCELADA = 'F'
                AND nm.TIPO_FATURAMENTO <> 'Outro'
                AND nm.NUM_FILIAL = {numerofilial}
                AND nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},23,59,0,0)
        ),
        cte_odrg AS (
            SELECT 
                *
            FROM
                OneD_regrasGMVFechamento odrg_2
            LEFT JOIN (
                SELECT
                    *
                FROM
                    OneD_lFechamentoFiliais odlff_2
            ) resultado2_2 ON resultado2_2.nomeProcesso = odrg_2.client 
            WHERE
                    odrg_2.mainidentifier = 'TM'
                AND odrg_2.subidentifier = 'MP'
        )
    SELECT
        GETDATE() as dataconsulta,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},23,59,0,0) as dataperiodofim,
        NULL as datapico,
        resultadofinal.nomeProcesso as filial,
        resultadofinal.numFilial as NUM_FILIAL,
        resultadofinal.mainidentifier as regra,
        resultadofinal.subidentifier as subregra,
        resultadofinal.descricao as descricao,
        resultadofinal.NOTAS_TOTAIS as quantidade,
        resultadofinal.VALOR_TOTAL AS valortotal,
        resultadofinal.takeRate as preco,
        'R$' as unidade,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinal,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorcommarkup,
        0 AS imposto,
        '%' AS unidadeimposto,
        0 AS valorimposto,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinalcomimposto
    FROM (
        SELECT 
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                (SELECT
                    SUM(cte_nmpm.VALOR_NOTA) AS valorfaturado
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.STATUS_WORKFLOW IN ('AG. EMBARQUE', 'AG. ENTREGA', 'AG. TRANSPORTADORA', 'ENTREGUE', 'EXTRAVIADO', 'FINALIZADO', 'INSUCESSO', 'EM DEVOLUÇÃO')   	        
                ) 
                / 
                (SELECT
                    COUNT(cte_nmpm.PEDIDOV) AS pedidosfaturados
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.FATURADO = 'T'
                    AND cte_nmpm.TIPO_FATURAMENTO = 'Faturamento'
                ) AS TICKET_MEDIO,
                BASE_CALCULO
            FROM
                cte_nmpm
            GROUP BY
                BASE_CALCULO
        ) resumo2
        INNER JOIN cte_odrg ON resumo2.BASE_CALCULO = cte_odrg.subidentifier
                        AND resumo2.TICKET_MEDIO > cte_odrg.lowerRange 
                        AND cte_odrg.lowerRange > cte_odrg.upperRange
        UNION
        SELECT 
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                (SELECT
                    SUM(cte_nmpm.VALOR_NOTA) AS valorfaturado
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.STATUS_WORKFLOW IN ('AG. EMBARQUE', 'AG. ENTREGA', 'AG. TRANSPORTADORA', 'ENTREGUE', 'EXTRAVIADO', 'FINALIZADO', 'INSUCESSO', 'EM DEVOLUÇÃO')   	        
                ) 
                / 
                (SELECT
                    COUNT(cte_nmpm.PEDIDOV) AS pedidosfaturados
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.FATURADO = 'T'
                    AND cte_nmpm.TIPO_FATURAMENTO = 'Faturamento'
                ) AS TICKET_MEDIO,
                BASE_CALCULO
            FROM
                cte_nmpm
            GROUP BY
                BASE_CALCULO
        ) resumo2
        INNER JOIN cte_odrg ON resumo2.BASE_CALCULO = cte_odrg.subidentifier
                        AND resumo2.TICKET_MEDIO BETWEEN cte_odrg.lowerRange AND cte_odrg.upperRange
    ) resultadofinal
    GROUP BY
        resultadofinal.nomeProcesso,
        resultadofinal.mainrule,
        resultadofinal.numFilial,
        resultadofinal.mainidentifier,
        resultadofinal.subidentifier,
        resultadofinal.descricao,
        resultadofinal.NOTAS_TOTAIS,
        resultadofinal.TICKET_MEDIO,
        resultadofinal.VALOR_TOTAL,
        resultadofinal.takeRate
''',
#ok - datas ok- mais atualizado
"GMV_TM_MF" :'''
    WITH 
        cte_nmpm AS (
            SELECT
                nm.*,
                CASE 
                    WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                    WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                    WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                    WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                    ELSE 'SP'
                END AS BASE_CALCULO,
                pm.N_PEDIDO_CLIENTE,
                pm.QUANTIDADE,
                pm.STATUS_WORKFLOW 
            FROM 
                nfs_millennium nm
            LEFT JOIN
                pedidos_millennium pm ON pm.PEDIDOV = nm.PEDIDOV
            WHERE
                nm.PEDIDOV IS NOT NULL
                AND nm.CANCELADA = 'F'
                AND nm.TIPO_FATURAMENTO <> 'Outro'
                AND nm.NUM_FILIAL = {numerofilial}
                AND nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},23,59,0,0)
        ),
        cte_odrg AS (
            SELECT 
                *
            FROM
                OneD_regrasGMVFechamento odrg_2
            LEFT JOIN (
                SELECT
                    *
                FROM
                    OneD_lFechamentoFiliais odlff_2
            ) resultado2_2 ON resultado2_2.nomeProcesso = odrg_2.client 
            WHERE
                    odrg_2.mainidentifier = 'TM'
                AND odrg_2.subidentifier = 'MF'
        )
    SELECT
        GETDATE() as dataconsulta,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},23,59,0,0) as dataperiodofim,
        NULL as datapico,
        resultadofinal.nomeProcesso as filial,
        resultadofinal.numFilial as NUM_FILIAL,
        resultadofinal.mainidentifier as regra,
        resultadofinal.subidentifier as subregra,
        resultadofinal.descricao as descricao,
        resultadofinal.NOTAS_TOTAIS as quantidade,
        resultadofinal.VALOR_TOTAL AS valortotal,
        resultadofinal.takeRate as preco,
        'R$' as unidade,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinal,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorcommarkup,
        0 AS imposto,
        '%' AS unidadeimposto,
        0 AS valorimposto,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinalcomimposto
    FROM (
        SELECT 
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                (SELECT
                    SUM(cte_nmpm.VALOR_NOTA) AS valorfaturado
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.STATUS_WORKFLOW IN ('AG. EMBARQUE', 'AG. ENTREGA', 'AG. TRANSPORTADORA', 'ENTREGUE', 'EXTRAVIADO', 'FINALIZADO', 'INSUCESSO', 'EM DEVOLUÇÃO')   	        
                ) 
                / 
                (SELECT
                    COUNT(cte_nmpm.PEDIDOV) AS pedidosfaturados
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.FATURADO = 'T'
                    AND cte_nmpm.TIPO_FATURAMENTO = 'Faturamento'
                ) AS TICKET_MEDIO,
                BASE_CALCULO
            FROM
                cte_nmpm
            GROUP BY
                BASE_CALCULO
        ) resumo2
        INNER JOIN cte_odrg ON resumo2.BASE_CALCULO = cte_odrg.subidentifier
                        AND resumo2.TICKET_MEDIO > cte_odrg.lowerRange 
                        AND cte_odrg.lowerRange > cte_odrg.upperRange
        UNION
        SELECT 
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                (SELECT
                    SUM(cte_nmpm.VALOR_NOTA) AS valorfaturado
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.STATUS_WORKFLOW IN ('AG. EMBARQUE', 'AG. ENTREGA', 'AG. TRANSPORTADORA', 'ENTREGUE', 'EXTRAVIADO', 'FINALIZADO', 'INSUCESSO', 'EM DEVOLUÇÃO')   	        
                ) 
                / 
                (SELECT
                    COUNT(cte_nmpm.PEDIDOV) AS pedidosfaturados
                FROM
                    cte_nmpm
                WHERE
                    cte_nmpm.FATURADO = 'T'
                    AND cte_nmpm.TIPO_FATURAMENTO = 'Faturamento'
                ) AS TICKET_MEDIO,
                BASE_CALCULO
            FROM
                cte_nmpm
            GROUP BY
                BASE_CALCULO
        ) resumo2
        INNER JOIN cte_odrg ON resumo2.BASE_CALCULO = cte_odrg.subidentifier
                        AND resumo2.TICKET_MEDIO BETWEEN cte_odrg.lowerRange AND cte_odrg.upperRange
    ) resultadofinal
    GROUP BY
        resultadofinal.nomeProcesso,
        resultadofinal.mainrule,
        resultadofinal.numFilial,
        resultadofinal.mainidentifier,
        resultadofinal.subidentifier,
        resultadofinal.descricao,
        resultadofinal.NOTAS_TOTAIS,
        resultadofinal.TICKET_MEDIO,
        resultadofinal.VALOR_TOTAL,
        resultadofinal.takeRate
''',
#ok - datas ok- mais atualizado
"GMV_SR_MP" : '''
    SELECT 
        GETDATE() as dataconsulta,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
        NULL as datapico,
        resultadofinal.nomeProcesso as filial,
        resultadofinal.numFilial as NUM_FILIAL,
        resultadofinal.mainrule as regra,
        resultadofinal.mainidentifier as subregra,
        resultadofinal.descricao as descricao,
        NULL as quantidade,
        resultadofinal.VALOR_TOTAL AS valortotal,
        resultadofinal.takeRate as preco,
        'R$' as unidade,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinal,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorcommarkup,
        0 AS imposto,
        '%' AS unidadeimposto,
        0 AS valorimposto,
        SUM(COALESCE(resultadofinal.VALOR_TOTAL, 0) * COALESCE(resultadofinal.takeRate, 0)) AS valorfinalcomimposto
    FROM (
        SELECT
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                BASE_CALCULO
            FROM (
                SELECT
                    *
                FROM (
                    SELECT
                        nm.NOTA,
                        nm.NUM_FILIAL,
                        pm.N_PEDIDO_CLIENTE AS ORDERID,
                        CASE 
                            WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                            WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                            WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                            WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                            ELSE 'SP'
                        END AS BASE_CALCULO,
                        nm.VALOR_NOTA,
                        pm.QUANTIDADE,
                        nm.EVENTO,
                        nm.VALOR_NOTA/pm.QUANTIDADE AS TICKET_MEDIO
                    FROM
                        nfs_millennium nm
                    LEFT JOIN pedidos_millennium pm ON nm.PEDIDOV = pm.PEDIDOV
                    WHERE	
                        nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                       AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                        AND
                        nm.NUM_FILIAL = {numerofilial}
                        AND
                        nm.PEDIDOV IS NOT NULL
                        AND
                        nm.VALOR_NOTA > 0 --remove as notas de recebimento de mercadorias
                ) resultado
            ) as resultado4
            GROUP BY
                BASE_CALCULO
        ) resultado5
        INNER JOIN (
                SELECT 
                    *
                FROM
                    OneD_regrasGMVFechamento odrg
                INNER JOIN (
                    SELECT
                        *
                    FROM
                        OneD_lFechamentoFiliais odlff
                    WHERE
                        odlff.numFilial = {numerofilial}
                    ) resultado2 ON resultado2.nomeProcesso = odrg.client 
                ) resultado3 ON resultado5.BASE_CALCULO = resultado3.subidentifier
                            AND resultado5.VALOR_TOTAL BETWEEN resultado3.lowerRange AND resultado3.upperRange
                            AND resultado3.mainidentifier = 'SR'
                            AND resultado3.subidentifier = 'MP'
        UNION
        SELECT
            *
        FROM (
            SELECT
                SUM(VALOR_NOTA) AS VALOR_TOTAL,
                COUNT(NOTA) AS NOTAS_TOTAIS,
                BASE_CALCULO
            FROM (
                SELECT
                    *
                FROM (
                    SELECT
                        nm.NOTA,
                        nm.NUM_FILIAL,
                        pm.N_PEDIDO_CLIENTE AS ORDERID,
                        CASE 
                            WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'MANUAL'
                            WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'COPIADO'
                            WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'MP'
                            WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'MP'
                            ELSE 'SP'
                        END AS BASE_CALCULO,
                        nm.VALOR_NOTA,
                        pm.QUANTIDADE,
                        nm.EVENTO,
                        nm.VALOR_NOTA/pm.QUANTIDADE AS TICKET_MEDIO
                    FROM
                        nfs_millennium nm
                    LEFT JOIN pedidos_millennium pm ON nm.PEDIDOV = pm.PEDIDOV
                    WHERE	
                        nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                       AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                        AND
                        nm.NUM_FILIAL = {numerofilial}
                        AND
                        nm.PEDIDOV IS NOT NULL
                        AND
                        nm.VALOR_NOTA > 0 --remove as notas de recebimento de mercadorias
                ) resultado
            ) as resultado4
            GROUP BY
                BASE_CALCULO
        ) resultado5
        INNER JOIN (
            SELECT 
                *
            FROM
                OneD_regrasGMVFechamento odrg
            INNER JOIN (
                SELECT
                    *
                FROM
                    OneD_lFechamentoFiliais odlff
                WHERE
                    odlff.numFilial = {numerofilial}
                ) resultado2 ON resultado2.nomeProcesso = odrg.client 
            ) resultado3 ON resultado5.BASE_CALCULO = resultado3.subidentifier
                        AND resultado5.VALOR_TOTAL > resultado3.lowerRange 
                        AND resultado3.lowerRange > resultado3.upperRange
                        AND resultado3.mainidentifier = 'SR'
                        AND resultado3.subidentifier = 'MP'
    ) resultadofinal
    GROUP BY
        resultadofinal.nomeProcesso,
        resultadofinal.numFilial,
        resultadofinal.mainrule,
        resultadofinal.mainidentifier,
        resultadofinal.descricao,
        resultadofinal.VALOR_TOTAL,
        resultadofinal.takeRate
''',

################################################### - SAC - ###########################################################
#atualizado - imposto ok - datas ok
"SAC_FX" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT	
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            NULL as quantidade,
            SUM(resultado.price) AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (COALESCE(SUM(resultado.price), 0)) AS valorfinal
        FROM
            (
                SELECT 
                    *
                FROM 
                    OneD_regrasOtherFechamento odrof
                LEFT JOIN
                    (
                        SELECT
                            av.CONTA AS nomeVTEX,
                            odlff.numFilial,
                            odlff.nomeProcesso 
                        FROM
                            OneD_lFechamentoFiliais odlff 
                        LEFT JOIN
                            acessos_vtex av 
                            ON
                            odlff.nomeFilialVtex = av.NOME_FILIAL 
                    ) resultado1
                    ON
                    resultado1.nomeProcesso = odrof.client
                WHERE
                    odrof.subrule = 'SAC_FX'
                    AND 
                    odrof.mainrule = 'SAC'
            ) resultado
        WHERE 
            resultado.numFilial = {numerofilial}
        GROUP BY
            resultado.client,
            resultado.mainrule,
            resultado.numFilial,
            resultado.subrule,
            resultado.item,
            resultado.price,
            resultado.unit
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'SAC'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',
#atualizado - imposto ok - datas ok
"SAC_TVGMV" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT	
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            NULL as quantidade,
            SUM(pv.totalValue) AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (COALESCE(resultado.price/100, 0) * COALESCE(SUM(pv.totalValue),0)) AS valorfinal
        FROM
            pedidos_vtex pv 
        LEFT JOIN (
                SELECT 
                    *
                FROM 
                    OneD_regrasOtherFechamento odrof
                LEFT JOIN
                    (
                        SELECT
                            av.CONTA AS nomeVTEX,
                            odlff.numFilial,
                            odlff.nomeProcesso 
                        FROM
                            OneD_lFechamentoFiliais odlff 
                        LEFT JOIN
                            acessos_vtex av 
                            ON
                            odlff.nomeFilialVtex = av.NOME_FILIAL 
                    ) resultado1
                    ON
                    resultado1.nomeProcesso = odrof.client
                WHERE
                    odrof.mainrule = 'SAC'
                    AND 
                    odrof.subrule = 'SAC_TVGMV'
            ) resultado
            ON
            pv.accountHostname = resultado.client
        WHERE 
            pv.invoicedDate BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                              AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
            AND
            pv.marketingDataCoupon LIKE '%televendas%'
            AND
            resultado.numFilial = {numerofilial}
            AND
            pv.status IN ('invoiced', 'handling', 'ready-for-handling', 'payment-approved', 'window-to-cancel', 'start-handling')
        GROUP BY
            resultado.client,
            resultado.numFilial,
            resultado.mainrule,
            resultado.subrule,
            resultado.item,
            resultado.price,
            resultado.unit
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'SAC'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',
#atualizado - imposto ok - datas ok
"SAC_HRX" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            CASE
                WHEN resultado2.[ID_SAC] = 'Sim' 
                    THEN resultado2.[N_HRX]
                ELSE 0
            END AS quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (
                COALESCE(resultado.price, 0) *     
                    CASE
                        WHEN resultado2.[ID_SAC] = 'Sim' 
                            THEN resultado2.[N_HRX]
                        ELSE 0
                    END
            ) AS valorfinal 
        FROM 
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'SAC_HRX'
                AND 
                odrof.mainrule = 'SAC'
            ) resultado
        LEFT JOIN 
            ( 
                SELECT
                    *
                FROM 
                    (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Cliente ORDER BY id DESC) AS num_linha
                    FROM 
                        OneD_valoresFormulario odvf
                    WHERE
                       odvf.[mes] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                      AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                    ) as resultado3
                WHERE 
                    num_linha = 1
            ) resultado2
            ON
            UPPER(resultado2.Cliente) = UPPER(resultado.client)
        WHERE
            resultado.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'SAC'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - VTEX - ##########################################################
#atualizado - imposto ok - datas ok
"AA_FIXO" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            NULL as quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (COALESCE(resultado.price, 0)) AS valorfinal
        FROM 
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                OneD_lFechamentoFiliais odlff 
                ON
                odlff.nomeProcesso = odrof.client
            WHERE
                odlff.numFilial = {numerofilial}
                AND 
                odrof.subrule = 'AA_FIXO'
            ) resultado
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'VTEX'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',
#atualizado - imposto ok - datas ok
"RVS_PDR": '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as filial,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            NULL as quantidade,
            SUM(pv.totalValue) AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (COALESCE(resultado.price/100, 0) * COALESCE(SUM(pv.totalValue),0)) AS valorfinal
        FROM 
            pedidos_vtex pv 	
        LEFT JOIN
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'RVS_PDR'
            ) resultado ON UPPER(pv.accountHostname) = UPPER(resultado.nomeVTEX)
        WHERE
            pv.invoicedDate BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
            AND
            pv.status IN ('invoiced', 'handling', 'ready-for-handling', 'payment-approved', 'window-to-cancel', 'start-handling')
            AND
            resultado.numFilial = {numerofilial}
        GROUP BY 
            resultado.client,
            resultado.numFilial,
            resultado.mainrule,
            resultado.subrule,
            resultado.item,
            resultado.price,
            resultado.unit
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'VTEX'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',
#atualizado - imposto ok - datas ok
"WTL_PDR" :'''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            COUNT(DISTINCT pv.sellersName) as quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (COALESCE(resultado.price, 0) * COALESCE(COUNT(DISTINCT pv.sellersName),0)) AS valorfinal
        FROM 
            pedidos_vtex pv 	
        LEFT JOIN
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'WTL_PDR'
            ) resultado ON UPPER(pv.accountHostname) = UPPER(resultado.nomeVTEX)
        WHERE
            pv.invoicedDate BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
            AND
            pv.status IN ('invoiced', 'handling', 'ready-for-handling', 'payment-approved', 'window-to-cancel', 'start-handling')
            AND
            resultado.numFilial = {numerofilial}
        GROUP BY 
            resultado.client,
            resultado.numFilial,
            resultado.mainrule,
            resultado.subrule,
            resultado.item,
            resultado.price,
            resultado.unit
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'VTEX'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - CADPRODUTOS - ###################################################
#atualizado - imposto ok - datas ok
"CAD_PDR" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            CASE
                WHEN resultado2.[ID_CADPROD] = 'Sim' 
                    THEN resultado2.[N_CADPROD]
                ELSE 0
            END AS quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (
                COALESCE(resultado.price, 0) *     
                    CASE
                        WHEN resultado2.[ID_CADPROD] = 'Sim' 
                            THEN resultado2.[N_CADPROD]
                        ELSE 0
                    END
            ) AS valorfinal 
        FROM 
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'CAD_PDR'
                AND 
                odrof.mainrule = 'CADPROD'
            ) resultado
        LEFT JOIN 
            ( 
                SELECT
                    *
                FROM 
                    (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Cliente ORDER BY id DESC) AS num_linha
                    FROM 
                        OneD_valoresFormulario odvf
                    WHERE
                        odvf.[mes] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                       AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                    ) as resultado3
                WHERE 
                    num_linha = 1
            ) resultado2
            ON
            UPPER(resultado2.Cliente) = UPPER(resultado.client)
        WHERE
            resultado.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'CADPROD'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - SABADOS - #######################################################
#atualizado - imposto ok - datas ok
"SAB_PDR" : '''
    WITH 
        rules 
            AS 
                (
                    SELECT 
                        *
                    FROM 
                        OneD_regrasOtherFechamento odrof
                    LEFT JOIN
                        (
                            SELECT
                                av.CONTA AS nomeVTEX,
                                odlff.numFilial,
                                odlff.nomeProcesso 
                            FROM
                                OneD_lFechamentoFiliais odlff 
                            LEFT JOIN
                                acessos_vtex av 
                                ON
                                odlff.nomeFilialVtex = av.NOME_FILIAL 
                        ) resultado1
                        ON
                        resultado1.nomeProcesso = odrof.client
                    WHERE
                        (
                            odrof.subrule = 'SAB_PDR_VAL'
                            OR 
                            odrof.subrule = 'SAB_PDR_QNT'
                        )
                        AND 
                        odrof.mainrule = 'SAB'
                    )
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            rules.client as filial,
            rules.numFIlial as NUM_FILIAL,
            rules.mainrule as regra,
            'SAB_PDR' as subregra,
            'Trabalho aos Sabados' as descricao,
            CASE
                WHEN resultado2.[ID_SAB] = 'Sim' 
                    THEN 
                        CASE
                            WHEN resultado2.[N_SABQNT] / (
                                                                                SELECT
                                                                                    rules.price
                                                                                FROM
                                                                                    rules
                                                                                WHERE
                                                                                    rules.subrule = 'SAB_PDR_QNT'
                                                                            ) > 1
                                THEN CEILING (resultado2.[N_SABQNT] / (
                                                                                SELECT
                                                                                    rules.price
                                                                                FROM
                                                                                    rules
                                                                                WHERE
                                                                                    rules.subrule = 'SAB_PDR_QNT'
                                                                            ))
                            ELSE 1
                        END
                ELSE 0
            END AS quantidade,
            NULL AS valortotal,
            (
                SELECT
                    rules.price
                FROM
                    rules
                WHERE
                    rules.subrule = 'SAB_PDR_VAL'
            ) AS preco,
            'R$' as unidade,
            (
                (
                    CASE
                        WHEN resultado2.[ID_SAB] = 'Sim' 
                            THEN 
                                CASE
                                    WHEN resultado2.[N_SABQNT] / (
                                                                                        SELECT
                                                                                            rules.price
                                                                                        FROM
                                                                                            rules
                                                                                        WHERE
                                                                                            rules.subrule = 'SAB_PDR_QNT'
                                                                                    ) > 1
                                        THEN CEILING (resultado2.[N_SABQNT] / (
                                                                                        SELECT
                                                                                            rules.price
                                                                                        FROM
                                                                                            rules
                                                                                        WHERE
                                                                                            rules.subrule = 'SAB_PDR_QNT'
                                                                                    ))
                                    ELSE 1
                                END
                        ELSE 0
                    END
                )
                *
                (
                    SELECT
                        rules.price
                    FROM
                        rules
                    WHERE
                        rules.subrule = 'SAB_PDR_VAL'
                )
            ) AS valorfinal 
        FROM 
            rules
        LEFT JOIN 
            ( 
                SELECT
                    *
                FROM 
                    (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Cliente ORDER BY id DESC) AS num_linha
                    FROM 
                        OneD_valoresFormulario odvf
                    WHERE
                        odvf.[mes] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                       AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                    ) as resultado3
                WHERE 
                    num_linha = 1
            ) resultado2
            ON
            UPPER(resultado2.Cliente) = UPPER(rules.client)
        WHERE
            rules.numFilial = {numerofilial}
        GROUP BY
            rules.client,
            rules.numFIlial,
            rules.mainrule,
            resultado2.[ID_SAB],
            resultado2.[N_SABQNT]
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'CADPROD'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - INVENTARIOS - ###################################################
#atualizado - imposto ok - datas ok
"INV_PDR" : '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            CASE
                WHEN resultado2.[ID_INVMENSAL] = 'Sim' 
                    THEN resultado2.[N_INVMENSAL]
                ELSE 0
            END AS quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (
                COALESCE(resultado.price, 0) *     
                    CASE
                        WHEN resultado2.[ID_INVMENSAL] = 'Sim' 
                            THEN resultado2.[N_INVMENSAL]
                        ELSE 0
                    END
            ) AS valorfinal 
        FROM 
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'INV_PDR'
                AND 
                odrof.mainrule = 'INV'
            ) resultado
        LEFT JOIN 
            ( 
                SELECT
                    *
                FROM 
                    (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Cliente ORDER BY id DESC) AS num_linha
                    FROM 
                        OneD_valoresFormulario odvf
                    WHERE
                        odvf.[mes] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                       AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                    ) as resultado3
                WHERE 
                    num_linha = 1
            ) resultado2
            ON
            UPPER(resultado2.Cliente) = UPPER(resultado.client)
        WHERE
            resultado.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'INV'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - REEMBOLSO - #####################################################
#atualizado - imposto ok - datas ok
"RMB_PDR" :'''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            1 AS quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            1 * resultado.price AS valorfinal 
        FROM 
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'RMB_PDR'
                AND
                odrof.mainrule = 'REEMBOLSO'
            ) resultado
        WHERE
            resultado.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'RMB'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

################################################### - ESTORNO - #######################################################
#atualizado - imposto ok - datas ok
"EST_PDR": '''
    SELECT
        resultado6.*,
        0 AS markup,
        '%' AS unidademarkup,
        0 AS valormarkup,
        resultado6.valorfinal AS valorcommarkup,
        resultadofrete2.price AS imposto,
        resultadofrete2.unit AS unidadeimposto,
        resultado6.valorfinal * (resultadofrete2.price/100) AS valorimposto,
        resultado6.valorfinal * (1 + (resultadofrete2.price/100)) AS valorfinalcomimposto
    FROM
        (
        SELECT
            GETDATE() as dataconsulta,
            DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
            DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
            NULL as datapico,
            resultado.client as filial,
            resultado.numFilial as NUM_FILIAL,
            resultado.mainrule as regra,
            resultado.subrule as subregra,
            resultado.item as descricao,
            CASE
                WHEN resultado2.[ID_ESTORNOS] = 'Sim' 
                    THEN 1
                ELSE
                    0
            END AS quantidade,
            NULL AS valortotal,
            resultado.price as preco,
            resultado.unit as unidade,
            (
                COALESCE(resultado.price, 0) *     
                    CASE
                        WHEN resultado2.[ID_ESTORNOS] = 'Sim' 
                            THEN 1
                        ELSE
                            0
                    END
            ) AS valorfinal 
        FROM 
            (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultado1
                ON
                resultado1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'EST_PDR'
                AND
                odrof.mainrule = 'ESTORNO'
            ) resultado
        LEFT JOIN 
            ( 
                SELECT
                    *
                FROM 
                    (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Cliente ORDER BY id DESC) AS num_linha
                    FROM 
                        OneD_valoresFormulario odvf
                    WHERE
                        odvf.[mes] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                       AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
                    ) as resultado3
                WHERE 
                    num_linha = 1
            ) resultado2
            ON
            UPPER(resultado2.Cliente) = UPPER(resultado.client)
        WHERE
            resultado.numFilial = {numerofilial}
        ) resultado6
    LEFT JOIN
        (
            SELECT 
                *
            FROM 
                OneD_regrasOtherFechamento odrof
            LEFT JOIN
                (
                    SELECT
                        av.CONTA AS nomeVTEX,
                        odlff.numFilial,
                        odlff.nomeProcesso 
                    FROM
                        OneD_lFechamentoFiliais odlff 
                    LEFT JOIN
                        acessos_vtex av 
                        ON
                        odlff.nomeFilialVtex = av.NOME_FILIAL 
                ) resultadofrete1
                ON
                resultadofrete1.nomeProcesso = odrof.client
            WHERE
                odrof.subrule = 'RMB'
                AND 
                odrof.mainrule = 'IMPOSTO'
        ) resultadofrete2
        ON
        resultadofrete2.numFilial = resultado6.NUM_FILIAL 
''',

#######################################################################################################################
########################################## - Relatórios analíticos - ##################################################
#######################################################################################################################


"GMV_REL" : '''
    SELECT
        'GMV' AS REGRA,
        {numerofilial} AS NUM_FILIAL,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
        nm.COD_OPERACAO AS GMV_COD_OPERACAO,
        nm.NOTA AS GMV_NOTA,
        nm.DATA_NF AS GMV_DATA_NF,
        nm.FILIAL AS GMV_FILIAL,
        nm.NUM_FILIAL AS GMV_NUM_FILIAL,
        pm.N_PEDIDO_CLIENTE AS GMV_OrderID,
        CASE 
            WHEN pm.ORIGEM_PEDIDO IN ('MANUAL') THEN 'GMV manual'
            WHEN pm.ORIGEM_PEDIDO IN ('COPIADO') THEN 'GMV copiado'
            WHEN pm.ORIGEM_PEDIDO NOT IN ('vtex', 'Netsuite', 'Magento', 'WEB_SITE', ' Shopify', 'WEB SITE', 'tray', 'FTP', 'SHOPPIFY') THEN 'GMV market place'
            WHEN pm.ORIGEM_PEDIDO LIKE ('%recebido do sistema%') THEN 'GMV market place'
            ELSE 'GMV site proprio'
        END AS GMV_BASE_CALCULO,
        nm.VALOR_NOTA AS GMV_VALOR_NOTA,
        pm.QUANTIDADE AS GMV_QUANTIDADE,
        pm.COD_PEDIDOV AS GMV_COD_PEDIDOV,
        pm.NOME_CLIENTE AS GMV_NOME_CLIENTE,
        pm.B2B_B2C AS GMV_B2B_B2C,
        pm.CNPJ AS GMV_CNPJ,
        pm.CPF AS GMV_CPF,
        nm.STATUS AS GMV_STATUS,
        nm.TIPO_OPERACAO AS GMV_TIPO_OPERACAO,
        nm.CHAVE_NFE AS GMV_CHAVE_NFE,
        nm.IDRECIBO AS GMV_IDRECIBO,
        nm.IDPROTOCOLO AS GMV_IDPROTOCOLO,
        nm.TIPOEMISNFE AS GMV_TIPOEMISNFE,
        nm.ID_NOTA AS GMV_ID_NOTA,
        nm.CFOP AS GMV_CFOP,
        nm.SERIE AS GMV_SERIE,
        nm.OBS AS GMV_OBS,

        NULL AS ARMZ_DATA_INCLUSAO,
        NULL AS ARMZ_FILIAL,
        NULL AS ARMZ_AVARIA,
        NULL AS ARMZ_PALETE,
        NULL AS ARMZ_PRATELEIRA,
        NULL AS ARMZ_BIN,
        NULL AS ARMZ_TOTAL,

        NULL AS MOV_NUM_FILIAL,
        NULL AS MOV_FILIAL,
        NULL AS MOV_CATEGORIA_PESO,
        NULL AS MOV_COD_PRODUTO,
        NULL AS MOV_PESO,
        NULL AS MOV_RECEBIMENTO,
        NULL AS MOV_DEVOLUCAO,
        NULL AS MOV_EXPEDICAO,
        NULL AS MOV_NOTA,

        NULL AS ESTDIA_NUM_FILIAL,
        NULL AS ESTDIA_FILIAL,
        NULL AS ESTDIA_DATA_MOVIMENTO,
        NULL AS ESTDIA_QTDSKU,
        NULL AS ESTDIA_QTDPECAS,
        NULL AS ESTDIA_QTDAPTOBLOCADO,
        NULL AS ESTDIA_QTDAPTOPICKING,
        NULL AS ESTDIA_QTDAPTOPULMAO,
        NULL AS ESTDIA_VALORESTOQUE,

        NULL AS FRTCTE_NUM_CTE,
        NULL AS FRTCTE_NUM_PEDIDO,
        NULL AS FRTCTE_VALOR_FRETE,
        NULL AS FRTCTE_DATA_CTE,
        NULL AS FRTCTE_NF,
        NULL AS FRTCTE_VALOR_NF,

        NULL AS FRTCOR_NUMERO_OBJETO,
        NULL AS FRTCOR_PEDIDO,
        NULL AS FRTCOR_VALOR_FRETE,
        NULL AS FRTCOR_DATA_POSTAGEM,
        NULL AS FRTCOR_NUM_FILIAL,
        NULL AS FRTCOR_NUM_NF,
        NULL AS FRTCOR_VALOR_NF
    FROM
        nfs_millennium nm
    LEFT JOIN pedidos_millennium pm ON nm.PEDIDOV = pm.PEDIDOV
    WHERE
        nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                        AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
        AND
        nm.NUM_FILIAL = {numerofilial}
        AND
        nm.PEDIDOV IS NOT NULL
''',


"ARMZ_REL" : '''
    SELECT
        'ARMZ' AS REGRA,
        {numerofilial}	AS NUM_FILIAL,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,
        
        NULL AS GMV_COD_OPERACAO,
        NULL AS GMV_NOTA,
        NULL AS GMV_DATA_NF,
        NULL AS GMV_FILIAL,
        NULL AS GMV_NUM_FILIAL,
        NULL AS GMV_OrderID,
        NULL AS GMV_BASE_CALCULO,
        NULL AS GMV_VALOR_NOTA,
        NULL AS GMV_QUANTIDADE,
        NULL AS GMV_COD_PEDIDOV,
        NULL AS GMV_NOME_CLIENTE,
        NULL AS GMV_B2B_B2C,
        NULL AS GMV_CNPJ,
        NULL AS GMV_CPF,
        NULL AS GMV_STATUS,
        NULL AS GMV_TIPO_OPERACAO,
        NULL AS GMV_CHAVE_NFE,
        NULL AS GMV_IDRECIBO,
        NULL AS GMV_IDPROTOCOLO,
        NULL AS GMV_TIPOEMISNFE,
        NULL AS GMV_ID_NOTA,
        NULL AS GMV_CFOP,
        NULL AS GMV_SERIE,
        NULL AS GMV_OBS,

        pivotado.data_inclusao AS ARMZ_DATA_INCLUSAO,
        pivotado.filial AS ARMZ_FILIAL,
        pivotado.Avaria AS ARMZ_AVARIA,
        pivotado.Palete AS ARMZ_PALETE,
        pivotado.Prateleira AS ARMZ_PRATELEIRA,
        pivotado.Bin AS ARMZ_BIN,
        COALESCE (Avaria,0) + COALESCE (Palete,0) + COALESCE (Prateleira,0) + COALESCE (Bin,0) AS ARMZ_TOTAL,

        NULL AS MOV_NUM_FILIAL,
        NULL AS MOV_FILIAL,
        NULL AS MOV_CATEGORIA_PESO,
        NULL AS MOV_COD_PRODUTO,
        NULL AS MOV_PESO,
        NULL AS MOV_RECEBIMENTO,
        NULL AS MOV_DEVOLUCAO,
        NULL AS MOV_EXPEDICAO,
        NULL AS MOV_NOTA,

        NULL AS ESTDIA_NUM_FILIAL,
        NULL AS ESTDIA_FILIAL,
        NULL AS ESTDIA_DATA_MOVIMENTO,
        NULL AS ESTDIA_QTDSKU,
        NULL AS ESTDIA_QTDPECAS,
        NULL AS ESTDIA_QTDAPTOBLOCADO,
        NULL AS ESTDIA_QTDAPTOPICKING,
        NULL AS ESTDIA_QTDAPTOPULMAO,
        NULL AS ESTDIA_VALORESTOQUE,

        NULL AS FRTCTE_NUM_CTE,
        NULL AS FRTCTE_NUM_PEDIDO,
        NULL AS FRTCTE_VALOR_FRETE,
        NULL AS FRTCTE_DATA_CTE,
        NULL AS FRTCTE_NF,
        NULL AS FRTCTE_VALOR_NF,

        NULL AS FRTCOR_NUMERO_OBJETO,
        NULL AS FRTCOR_PEDIDO,
        NULL AS FRTCOR_VALOR_FRETE,
        NULL AS FRTCOR_DATA_POSTAGEM,
        NULL AS FRTCOR_NUM_FILIAL,
        NULL AS FRTCOR_NUM_NF,
        NULL AS FRTCOR_VALOR_NF

    FROM (
        SELECT
            wvsqpds.datainclusao AS data_inclusao,
            wvsqpds.filial AS filial,
            SUM(wvsqpds.qtde) AS quantidade,
            odna.significado AS significado,
            odlff.numFilial AS NUM_FILIAL
        FROM
            wms_vSpQtdePosicaoDiarioSuccess wvsqpds 
        LEFT JOIN
            OneD_nomenclaturaARMZFechamento odna ON LEFT(wvsqpds.siglarua, 2) = odna.Sigla
        LEFT JOIN
            OneD_lFechamentoFiliais odlff ON odlff.codFilialMill = wvsqpds.filial
        WHERE
            wvsqpds.datainclusao BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                    AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
            AND
            odlff.numFilial = {numerofilial}
        GROUP BY
            odna.significado,
            wvsqpds.datainclusao,
            wvsqpds.filial,
            odlff.numFilial
    ) src
    PIVOT
    (
        SUM(src.quantidade)
        FOR src.significado IN ([Avaria], [Palete], [Prateleira], [Bin])
    ) pivotado
''',


"MOV_REL" : '''
    SELECT
        'MOV' AS REGRA,
        {numerofilial}	AS 	NUM_FILIAL,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,

        NULL AS GMV_COD_OPERACAO,
        NULL AS GMV_NOTA,
        NULL AS GMV_DATA_NF,
        NULL AS GMV_FILIAL,
        NULL AS GMV_NUM_FILIAL,
        NULL AS GMV_OrderID,
        NULL AS GMV_BASE_CALCULO,
        NULL AS GMV_VALOR_NOTA,
        NULL AS GMV_QUANTIDADE,
        NULL AS GMV_COD_PEDIDOV,
        NULL AS GMV_NOME_CLIENTE,
        NULL AS GMV_B2B_B2C,
        NULL AS GMV_CNPJ,
        NULL AS GMV_CPF,
        NULL AS GMV_STATUS,
        NULL AS GMV_TIPO_OPERACAO,
        NULL AS GMV_CHAVE_NFE,
        NULL AS GMV_IDRECIBO,
        NULL AS GMV_IDPROTOCOLO,
        NULL AS GMV_TIPOEMISNFE,
        NULL AS GMV_ID_NOTA,
        NULL AS GMV_CFOP,
        NULL AS GMV_SERIE,
        NULL AS GMV_OBS,

        NULL AS ARMZ_DATA_INCLUSAO,
        NULL AS ARMZ_FILIAL,
        NULL AS ARMZ_AVARIA,
        NULL AS ARMZ_PALETE,
        NULL AS ARMZ_PRATELEIRA,
        NULL AS ARMZ_BIN,
        NULL AS ARMZ_TOTAL,

        mfrfs.numFilial AS MOV_NUM_FILIAL,
        NULL AS MOV_FILIAL,
        mfrfs.categoria_peso AS MOV_CATEGORIA_PESO,
        mfrfs.cod_produto AS MOV_COD_PRODUTO,
        mfrfs.peso AS MOV_PESO,
        mfrfs.recebimento AS MOV_RECEBIMENTO,
        mfrfs.devolucao AS MOV_DEVOLUCAO,
        mfrfs.expedicao AS MOV_EXPEDICAO,
        mfrfs.nota AS MOV_NOTA,

        NULL AS ESTDIA_NUM_FILIAL,
        NULL AS ESTDIA_FILIAL,
        NULL AS ESTDIA_DATA_MOVIMENTO,
        NULL AS ESTDIA_QTDSKU,
        NULL AS ESTDIA_QTDPECAS,
        NULL AS ESTDIA_QTDAPTOBLOCADO,
        NULL AS ESTDIA_QTDAPTOPICKING,
        NULL AS ESTDIA_QTDAPTOPULMAO,
        NULL AS ESTDIA_VALORESTOQUE,

        NULL AS FRTCTE_NUM_CTE,
        NULL AS FRTCTE_NUM_PEDIDO,
        NULL AS FRTCTE_VALOR_FRETE,
        NULL AS FRTCTE_DATA_CTE,
        NULL AS FRTCTE_NF,
        NULL AS FRTCTE_VALOR_NF,

        NULL AS FRTCOR_NUMERO_OBJETO,
        NULL AS FRTCOR_PEDIDO,
        NULL AS FRTCOR_VALOR_FRETE,
        NULL AS FRTCOR_DATA_POSTAGEM,
        NULL AS FRTCOR_NUM_FILIAL,
        NULL AS FRTCOR_NUM_NF,
        NULL AS FRTCOR_VALOR_NF
    FROM 
        mill_fRelFechamentoSucess mfrfs 
    WHERE
        mfrfs.numFilial = {numerofilial}

''',


"ESTDIA_REL" : '''
    SELECT
        'ESTDIA' AS REGRA,
        {numerofilial}	AS 	NUM_FILIAL,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,

        NULL AS GMV_COD_OPERACAO,
        NULL AS GMV_NOTA,
        NULL AS GMV_DATA_NF,
        NULL AS GMV_FILIAL,
        NULL AS GMV_NUM_FILIAL,
        NULL AS GMV_OrderID,
        NULL AS GMV_BASE_CALCULO,
        NULL AS GMV_VALOR_NOTA,
        NULL AS GMV_QUANTIDADE,
        NULL AS GMV_COD_PEDIDOV,
        NULL AS GMV_NOME_CLIENTE,
        NULL AS GMV_B2B_B2C,
        NULL AS GMV_CNPJ,
        NULL AS GMV_CPF,
        NULL AS GMV_STATUS,
        NULL AS GMV_TIPO_OPERACAO,
        NULL AS GMV_CHAVE_NFE,
        NULL AS GMV_IDRECIBO,
        NULL AS GMV_IDPROTOCOLO,
        NULL AS GMV_TIPOEMISNFE,
        NULL AS GMV_ID_NOTA,
        NULL AS GMV_CFOP,
        NULL AS GMV_SERIE,
        NULL AS GMV_OBS,

        NULL AS ARMZ_DATA_INCLUSAO,
        NULL AS ARMZ_FILIAL,
        NULL AS ARMZ_AVARIA,
        NULL AS ARMZ_PALETE,
        NULL AS ARMZ_PRATELEIRA,
        NULL AS ARMZ_BIN,
        NULL AS ARMZ_TOTAL,

        NULL AS MOV_NUM_FILIAL,
        NULL AS MOV_FILIAL,
        NULL AS MOV_CATEGORIA_PESO,
        NULL AS MOV_COD_PRODUTO,
        NULL AS MOV_PESO,
        NULL AS MOV_RECEBIMENTO,
        NULL AS MOV_DEVOLUCAO,
        NULL AS MOV_EXPEDICAO,
        NULL AS MOV_NOTA,

        {numerofilial} AS ESTDIA_NUM_FILIAL,
        wvsseds.filial AS ESTDIA_FILIAL,
        wvsseds.datainclusao AS ESTDIA_DATA_MOVIMENTO,
        wvsseds.qtdsku AS ESTDIA_QTDSKU,
        wvsseds.qtdpecas AS ESTDIA_QTDPECAS,
        wvsseds.qtdaptoblocado  AS ESTDIA_QTDAPTOBLOCADO,
        wvsseds.qtdaptopicking AS ESTDIA_QTDAPTOPICKING,
        wvsseds.qtdaptopulmao AS ESTDIA_QTDAPTOPULMAO,
        wvsseds.valorestoque AS ESTDIA_VALORESTOQUE,

        NULL AS FRTCTE_NUM_CTE,
        NULL AS FRTCTE_NUM_PEDIDO,
        NULL AS FRTCTE_VALOR_FRETE,
        NULL AS FRTCTE_DATA_CTE,
        NULL AS FRTCTE_NF,
        NULL AS FRTCTE_VALOR_NF,

        NULL AS FRTCOR_NUMERO_OBJETO,
        NULL AS FRTCOR_PEDIDO,
        NULL AS FRTCOR_VALOR_FRETE,
        NULL AS FRTCOR_DATA_POSTAGEM,
        NULL AS FRTCOR_NUM_FILIAL,
        NULL AS FRTCOR_NUM_NF,
        NULL AS FRTCOR_VALOR_NF
    FROM
        wms_vSpSaldoEstoqueDiarioSuccess wvsseds
    LEFT JOIN
        OneD_lFechamentoFiliais odlff ON odlff.codFilialMill = wvsseds.filial
    WHERE
        wvsseds.datainclusao BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND  DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
        AND
        odlff.numFilial = {numerofilial}
''',


"FRTCTE_REL" : '''
    SELECT
        'FRTCTE' AS REGRA,
        {numerofilial}	AS 	NUM_FILIAL,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,

        NULL AS GMV_COD_OPERACAO,
        NULL AS GMV_NOTA,
        NULL AS GMV_DATA_NF,
        NULL AS GMV_FILIAL,
        NULL AS GMV_NUM_FILIAL,
        NULL AS GMV_OrderID,
        NULL AS GMV_BASE_CALCULO,
        NULL AS GMV_VALOR_NOTA,
        NULL AS GMV_QUANTIDADE,
        NULL AS GMV_COD_PEDIDOV,
        NULL AS GMV_NOME_CLIENTE,
        NULL AS GMV_B2B_B2C,
        NULL AS GMV_CNPJ,
        NULL AS GMV_CPF,
        NULL AS GMV_STATUS,
        NULL AS GMV_TIPO_OPERACAO,
        NULL AS GMV_CHAVE_NFE,
        NULL AS GMV_IDRECIBO,
        NULL AS GMV_IDPROTOCOLO,
        NULL AS GMV_TIPOEMISNFE,
        NULL AS GMV_ID_NOTA,
        NULL AS GMV_CFOP,
        NULL AS GMV_SERIE,
        NULL AS GMV_OBS,

        NULL AS ARMZ_DATA_INCLUSAO,
        NULL AS ARMZ_FILIAL,
        NULL AS ARMZ_AVARIA,
        NULL AS ARMZ_PALETE,
        NULL AS ARMZ_PRATELEIRA,
        NULL AS ARMZ_BIN,
        NULL AS ARMZ_TOTAL,

        NULL AS MOV_NUM_FILIAL,
        NULL AS MOV_FILIAL,
        NULL AS MOV_CATEGORIA_PESO,
        NULL AS MOV_COD_PRODUTO,
        NULL AS MOV_PESO,
        NULL AS MOV_RECEBIMENTO,
        NULL AS MOV_DEVOLUCAO,
        NULL AS MOV_EXPEDICAO,
        NULL AS MOV_NOTA,

        NULL AS ESTDIA_NUM_FILIAL,
        NULL AS ESTDIA_FILIAL,
        NULL AS ESTDIA_DATA_MOVIMENTO,
        NULL AS ESTDIA_QTDSKU,
        NULL AS ESTDIA_QTDPECAS,
        NULL AS ESTDIA_QTDAPTOBLOCADO,
        NULL AS ESTDIA_QTDAPTOPICKING,
        NULL AS ESTDIA_QTDAPTOPULMAO,
        NULL AS ESTDIA_VALORESTOQUE,

        cj.CTE AS FRTCTE_NUM_CTE,
        pm.COD_PEDIDOV AS FRTCTE_NUM_PEDIDO,
        CAST(cj.VALOR_PRESTACAO AS int) AS FRTCTE_VALOR_FRETE,
        cj.[DATA] AS FRTCTE_DATA_CTE,
        nm.NOTA AS FRTCTE_NF,
        nm.VALOR_NOTA AS FRTCTE_VALOR_NF,

        NULL AS FRTCOR_NUMERO_OBJETO,
        NULL AS FRTCOR_PEDIDO,
        NULL AS FRTCOR_VALOR_FRETE,
        NULL AS FRTCOR_DATA_POSTAGEM,
        NULL AS FRTCOR_NUM_FILIAL,
        NULL AS FRTCOR_NUM_NF,
        NULL AS FRTCOR_VALOR_NF

    FROM 
        ctes_jettax cj
    LEFT JOIN
        nfs_millennium nm 
        ON 
        cj.CHAVE_NFE = nm.CHAVE_NFE
    LEFT JOIN 
        pedidos_millennium pm 
        ON
        nm.PEDIDOV = pm.PEDIDOV 
    WHERE	
        nm.DATA_NF BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                    AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
        AND
        nm.NUM_FILIAL = {numerofilial}
''',


"FRTCOR_REL" : '''
    SELECT 
        'FRTCOR' AS REGRA,
        {numerofilial}	AS NUM_FILIAL,
        DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0) as dataperiodoinicio,
        DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0) as dataperiodofim,

        NULL AS GMV_COD_OPERACAO,
        NULL AS GMV_NOTA,
        NULL AS GMV_DATA_NF,
        NULL AS GMV_FILIAL,
        NULL AS GMV_NUM_FILIAL,
        NULL AS GMV_OrderID,
        NULL AS GMV_BASE_CALCULO,
        NULL AS GMV_VALOR_NOTA,
        NULL AS GMV_QUANTIDADE,
        NULL AS GMV_COD_PEDIDOV,
        NULL AS GMV_NOME_CLIENTE,
        NULL AS GMV_B2B_B2C,
        NULL AS GMV_CNPJ,
        NULL AS GMV_CPF,
        NULL AS GMV_STATUS,
        NULL AS GMV_TIPO_OPERACAO,
        NULL AS GMV_CHAVE_NFE,
        NULL AS GMV_IDRECIBO,
        NULL AS GMV_IDPROTOCOLO,
        NULL AS GMV_TIPOEMISNFE,
        NULL AS GMV_ID_NOTA,
        NULL AS GMV_CFOP,
        NULL AS GMV_SERIE,
        NULL AS GMV_OBS,

        NULL AS ARMZ_DATA_INCLUSAO,
        NULL AS ARMZ_FILIAL,
        NULL AS ARMZ_AVARIA,
        NULL AS ARMZ_PALETE,
        NULL AS ARMZ_PRATELEIRA,
        NULL AS ARMZ_BIN,
        NULL AS ARMZ_TOTAL,

        NULL AS MOV_NUM_FILIAL,
        NULL AS MOV_FILIAL,
        NULL AS MOV_CATEGORIA_PESO,
        NULL AS MOV_COD_PRODUTO,
        NULL AS MOV_PESO,
        NULL AS MOV_RECEBIMENTO,
        NULL AS MOV_DEVOLUCAO,
        NULL AS MOV_EXPEDICAO,
        NULL AS MOV_NOTA,

        NULL AS ESTDIA_NUM_FILIAL,
        NULL AS ESTDIA_FILIAL,
        NULL AS ESTDIA_DATA_MOVIMENTO,
        NULL AS ESTDIA_QTDSKU,
        NULL AS ESTDIA_QTDPECAS,
        NULL AS ESTDIA_QTDAPTOBLOCADO,
        NULL AS ESTDIA_QTDAPTOPICKING,
        NULL AS ESTDIA_QTDAPTOPULMAO,
        NULL AS ESTDIA_VALORESTOQUE,

        NULL AS FRTCTE_NUM_CTE,
        NULL AS FRTCTE_NUM_PEDIDO,
        NULL AS FRTCTE_VALOR_FRETE,
        NULL AS FRTCTE_DATA_CTE,
        NULL AS FRTCTE_NF,
        NULL AS FRTCTE_VALOR_NF,

        empm.NUMERO_OBJETO AS FRTCOR_NUMERO_OBJETO,
        empm.COD_PEDIDOV AS FRTCOR_PEDIDO,
        CAST(REPLACE(REPLACE(RIGHT(ofc.[Valor Liquido (R$)], LEN(ofc.[Valor Liquido (R$)]) -3), '.' , ''), ',', '.') AS float) AS FRTCOR_VALOR_FRETE,
        ofc.[Data da Postagem] AS FRTCOR_DATA_POSTAGEM,
        empm.NUM_FILIAL AS FRTCOR_NUM_FILIAL,
        empm.NUM_NF AS FRTCOR_NUM_NF,
        empm.VALOR_NF AS FRTCOR_VALOR_NF
    FROM
        OneD_faturaCorreios ofc
    LEFT JOIN
        (
            SELECT
                em.NUMERO_OBJETO AS NUMERO_OBJETO,
                pm.COD_PEDIDOV  AS COD_PEDIDOV,
                pm.NUM_FILIAL AS NUM_FILIAL,
                nm.NOTA AS NUM_NF,
                nm.VALOR_NOTA AS VALOR_NF
            FROM
                embarques_millennium em
            LEFT JOIN
                pedidos_millennium pm 
                ON
                pm.PEDIDOV = em.PEDIDOV
            LEFT JOIN 
                nfs_millennium nm 
                ON
                nm.PEDIDOV = pm.PEDIDOV 
        ) empm
        ON
        empm.NUMERO_OBJETO = ofc.[Numero da Etiqueta]
    WHERE
        ofc.[Data da Postagem] BETWEEN DATETIMEFROMPARTS ({startyear},{startmonth},{startday},0,0,0,0)
                                AND DATETIMEFROMPARTS ({endyear},{endmonth},{endday},0,0,0,0)
        AND
        empm.NUM_FILIAL = {numerofilial}
'''
}

sql_list_deprecated = {
#ok <- esse é o antigo
"MOV_PDR_alternativo" : '''
    SELECT
        GETDATE() as dataconsulta,
        NULL as datapico,
        regrasFechamento.numFilial as filial,
        regrasFechamento.mainrule as regra,
        regrasFechamento.subrule as subregra,
        regrasFechamento.item as descricao,
        tabela.valor as quantidade,
        NULL as valortotal,
        regrasFechamento.price as preco,
        regrasFechamento.unit + ' - ' + tabela.categoria_peso as unidade,
        tabela.valor * COALESCE(regrasFechamento.price, 0) AS valorfinal
    FROM (
        SELECT 
            valor,
            categoria_peso,
            numFilial,
            item
        FROM (
            SELECT
                SUM(recebimento) AS recebimento,
                SUM(expedicao) AS expedicao,
                SUM(devolucao) AS devolucao,
                mfrfs.categoria_peso,
                mfrfs.numFilial 
            FROM
                mill_fRelFechamentoSucess mfrfs 
            GROUP BY
                mfrfs.categoria_peso,
                mfrfs.numFilial
            ) resultado
        CROSS apply (
            VALUES 
                ('recebimento', recebimento),
                ('expedicao', expedicao),
                ('devolucao', devolucao)
        ) c (item, valor)
    ) as tabela
    LEFT JOIN (
        SELECT 
            *
        FROM 
            OneD_regrasOtherFechamento odrof
        LEFT JOIN
            (
                SELECT
                    av.CONTA AS nomeVTEX,
                    odlff.numFilial,
                    odlff.nomeProcesso 
                FROM
                    OneD_lFechamentoFiliais odlff 
                LEFT JOIN
                    acessos_vtex av 
                    ON
                    odlff.nomeFilialVtex = av.NOME_FILIAL 
            ) resultado1
            ON
            resultado1.nomeProcesso = odrof.client
        WHERE
            odrof.subrule IN ('REC_PDR', 'EXP_PDR', 'DEV_PDR')
        AND 
            odrof.mainrule = 'MOV'
    ) AS regrasFechamento ON UPPER(regrasFechamento.item) = Upper(tabela.item)
                        AND UPPER(regrasFechamento.criteria) = UPPER(tabela.categoria_peso)
                        AND regrasFechamento.numFilial = tabela.numFilial
    WHERE 
        regrasFechamento.numFilial = {numerofilial}
''',


#deprecated
"VTX_PDR" : '''
    SELECT	
        GETDATE() as dataconsulta,
        NULL as datapico,
        resultado.client as filial,
        resultado.mainrule as regra,
        resultado.subrule as subregra,
        resultado.item as descricao,
        NULL as quantidade,
        SUM(pv.totalValue) AS valortotal,
        resultado.price as preco,
        resultado.unit as unidade,
        (COALESCE(resultado.price/100,0) * COALESCE(SUM(pv.totalValue), 0)) AS valorfinal
    FROM
        pedidos_vtex pv
    LEFT JOIN
            (
            SELECT
                odlff.numFilial,
                odrof.client,
                odrof.price,
                odrof.unit,
                odrof.mainrule,
                odrof.subrule,
                odrof.item
            FROM
                OneD_regrasOtherFechamento odrof 
            LEFT JOIN
                OneD_lFechamentoFiliais odlff 
            ON
                odlff.nomeProcesso = odrof.client
            WHERE
                odrof.mainrule = 'VTEX'
                AND
                odrof.subrule = 'RVS_PDR'
        ) resultado
        ON
        resultado.client = pv.accountHostname  
    WHERE 
        pv.invoicedDate BETWEEN '{startdate}'
                            AND '{enddate}'
        AND
        resultado.numFilial = {numerofilial}
        AND
        pv.status IN ('invoiced', 'handling', 'ready-for-handling', 'payment-approved', 'window-to-cancel', 'start-handling')
    GROUP BY
        resultado.client,
        resultado.mainrule,
        resultado.subrule,
        resultado.item,
        resultado.price,
        resultado.unit
''',
}