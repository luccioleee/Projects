sql_list = {
"get_identifierData" : '''
    SELECT
        resultado.identifier,
        resultado.identifierValue
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY runQueryName ORDER BY runID DESC) AS num_linha
        FROM (
            SELECT
                *
            FROM
                nfo_successRunTable nsrt
            WHERE
                nsrt.runQueryName = '%s'
        ) AS intermediarioUm
    ) AS resultado
    WHERE
        resultado.num_linha = 1
''',
"get_repostasNpsData" : '''
    SELECT
        n.id_nps AS ID_NPS,
        n.score AS SCORE,
        n.dificuldade AS DIFICULDADE,
        n.recomenda AS RECOMENDA,
        n.navegacao AS NAVEGACAO,
        n.entrega AS ENTREGA,
        n.atendimento AS ATENDIMENTO,
        n.dificuldade_observacao AS DIFICULDADE_OBSERVACAO,
        n.navegacao_observacao AS NAVEGACAO_OBSERVACAO,
        n.entrega_observacao AS ENTREGA_OBSERVACAO,
        n.atendimento_observacao AS ATENDIMENTO_OBSERVACAO,
        auditTable.auditId AS auditId
    FROM
        nps n 
    INNER JOIN (
        SELECT
            resultado.id_nps,
            resultado.auditId
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_nps ORDER BY eventDate DESC) AS num_linha
            FROM (
                SELECT
                    *
                FROM 
                    audit_table at2 
                WHERE
                    at2.auditId > %s
            ) AS intermediarioUm
        ) AS resultado
        WHERE 
            resultado.num_linha = 1
    ) auditTable ON n.id_nps = auditTable.id_nps
''',
"get_emailsNpsData" : '''
    SELECT
        n.id_nps AS ID_NPS,
        n.cod_pedidov AS COD_PEDIDOV,
        n.filial AS NUM_FILIAL,
        NULL AS FILIAL,
        n.cod_filial AS COD_FILIAL,
        n.data_criacao AS DATA_CRIACAO,
        auditTable.auditId AS auditId
    FROM
        nps n 
    INNER JOIN (
        SELECT
            resultado.id_nps,
            resultado.auditId
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_nps ORDER BY eventDate DESC) AS num_linha
            FROM (
                SELECT
                    *
                FROM 
                    audit_table at2 
                WHERE
                    at2.auditId > %s
            ) AS intermediarioUm
        ) AS resultado
        WHERE 
            resultado.num_linha = 1
    ) auditTable ON n.id_nps = auditTable.id_nps  
''',
"merge_repostasNpsData" : '''
    MERGE respostas_nps_millennium WITH (HOLDLOCK) AS main
    USING (SELECT * FROM temp_respostas_nps_millenniumSuccess) AS temp
    ON (main.ID_NPS = temp.ID_NPS)
    WHEN MATCHED THEN
        UPDATE SET
            main.ID_NPS = temp.ID_NPS,
            main.SCORE = temp.SCORE,
            main.DIFICULDADE = temp.DIFICULDADE,
            main.RECOMENDA = temp.RECOMENDA,
            main.NAVEGACAO = temp.NAVEGACAO,
            main.ENTREGA = temp.ENTREGA,
            main.ATENDIMENTO = temp.ATENDIMENTO,
            main.DIFICULDADE_OBSERVACAO = temp.DIFICULDADE_OBSERVACAO,
            main.NAVEGACAO_OBSERVACAO = temp.NAVEGACAO_OBSERVACAO,
            main.ENTREGA_OBSERVACAO = temp.ENTREGA_OBSERVACAO,
            main.ATENDIMENTO_OBSERVACAO = temp.ATENDIMENTO_OBSERVACAO
    WHEN NOT MATCHED THEN
        INSERT (ID_NPS, SCORE, DIFICULDADE, RECOMENDA, NAVEGACAO, ENTREGA, ATENDIMENTO, DIFICULDADE_OBSERVACAO, NAVEGACAO_OBSERVACAO, ENTREGA_OBSERVACAO, ATENDIMENTO_OBSERVACAO)
        VALUES (temp.ID_NPS, temp.SCORE, temp.DIFICULDADE, temp.RECOMENDA, temp.NAVEGACAO, temp.ENTREGA, temp.ATENDIMENTO, temp.DIFICULDADE_OBSERVACAO, temp.NAVEGACAO_OBSERVACAO, temp.ENTREGA_OBSERVACAO, temp.ATENDIMENTO_OBSERVACAO);
''',
"merge_emailsNpsData" : '''
    MERGE emails_nps_millennium WITH (HOLDLOCK) AS main
    USING (SELECT * FROM temp_emails_nps_millenniumSuccess) AS temp
    ON (main.ID_NPS = temp.ID_NPS)
    WHEN MATCHED THEN
        UPDATE SET
            main.ID_NPS = temp.ID_NPS,
            main.COD_PEDIDOV = temp.COD_PEDIDOV,
            main.FILIAL = temp.FILIAL,
            main.NUM_FILIAL = temp.NUM_FILIAL,
            main.COD_FILIAL = temp.COD_FILIAL,
            main.DATA_CRIACAO = temp.DATA_CRIACAO
    WHEN NOT MATCHED THEN
        INSERT (ID_NPS, COD_PEDIDOV, FILIAL, COD_FILIAL, DATA_CRIACAO)
        VALUES (temp.ID_NPS, temp.COD_PEDIDOV, temp.FILIAL, temp.COD_FILIAL, temp.DATA_CRIACAO);
'''
}