sql_list = {
"getclientslist" : '''
    SELECT 
        *
    FROM
        nfo_meliClientTable mct
    WHERE
        mct.active = 'T'
''',
"getongointokenslist" : '''
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY refresh_id DESC) AS num_linha
        FROM 
            nfo_meliTokenTable mtt
    ) AS resultado
    WHERE
        resultado.num_linha = 1
'''
}