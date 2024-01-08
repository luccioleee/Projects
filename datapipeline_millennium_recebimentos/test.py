# %%
import json
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, select, MetaData, Table
import requests
import sqlalchemy as sa
import urllib
from datetime import date, datetime, timedelta
from threading import Thread

from sql_queries import sql_list


# %%
# to use with txt files
    # with open('output.txt', 'w') as text_file:
    #     string_list = 'data_movimento, data_nf, codnota, numfilial, codbarra, codproduto, produto, codcor, codtamanho, quantity, price, valor_total, custo_medio, fufilled_quantity, total_sold_quantity, outbound_quantity, status, last_nota_status, codfilial'
    #     text_file.write(string_list)
    #     text_file.write('\n')
    #     for each_filial in globals()["list_filials"]:
    #         for each_barra in each_filial.list_barras:
    #             for each_nota in each_barra.list_codnota:
    #                 string_list = ""
    #                 # string_list += f'data_movimento: {each_barra.data_movimento}, '
    #                 # string_list += f'data_nf: {each_barra.data_nf}, '
    #                 # string_list += f'nota: {each_barra.codnota}'
    #                 # string_list += f'filial: {each_filial.numfilial}, '
    #                 # string_list += f'BARRA: {each_barra.BARRA}, '
    #                 # string_list += f'COD_PRODUTO: {each_barra.COD_PRODUTO}, '
    #                 # string_list += f'produto: {each_barra.produto}, '
    #                 # string_list += f'COD_COR: {each_barra.COD_COR}, '
    #                 # string_list += f'tamanho: {each_barra.tamanho}, ' 
    #                 # string_list += f'quantidade: {each_barra.quantity} '
    #                 # string_list += f'preco: {each_barra.price} '
    #                 # string_list += f'valor_total: {each_barra.valor_total} '
    #                 # string_list += f'custo_medio: {each_barra.custo_medio} '
    #                 # string_list += f'fufilled_quantity: {each_barra.fufilled_quantity} '
    #                 # string_list += f'status: {each_barra.status} '
    #                 # string_list += f'last_nota_status: {each_barra.last_nota_status} '

    #                 string_list += f'{each_nota.data_movimento}, '
    #                 string_list += f'{each_nota.data_nf}, '
    #                 string_list += f'{each_nota.codnota}, '
    #                 string_list += f'{each_filial.numfilial}, '
    #                 string_list += f'{each_barra.codbarra}, '
    #                 string_list += f'{each_nota.codproduto}, '
    #                 string_list += f'{each_nota.produto}, '
    #                 string_list += f'{each_nota.codcor}, '
    #                 string_list += f'{each_nota.codtamanho}, ' 
    #                 string_list += f'{each_nota.quantity}, '
    #                 string_list += f'{each_nota.price}, '
    #                 string_list += f'{each_nota.valor_total}, '
    #                 string_list += f'{each_nota.custo_medio}, '
    #                 string_list += f'{each_nota.fufilled_quantity}, '
    #                 string_list += f'{each_nota.total_sold_quantity}, '
    #                 string_list += f'{each_nota.outbound_quantity}, '
    #                 string_list += f'{each_nota.status}, '
    #                 string_list += f'{each_nota.last_nota_status}, '
    #                 string_list += f'{each_nota.codfilial} '
    #                 text_file.write(string_list)
    #                 text_file.write('\n')

# %%
def getConnforMYSQL(f_data, accessType):
    list_dialects = pyodbc.drivers()

    for dialect in list_dialects:
        try:
            server = f_data[accessType]["server"]
            db = f_data[accessType]["database"]
            uid = f_data[accessType]["uid"]
            pwd = f_data[accessType]["pwd"]
            driver = f_data[accessType]["dialect_driver"]
            port = f_data[accessType]["port"]

            if accessType == "azureAccess":
                if dialect in f_data[accessType]["list_workingDialects"]:
                    print(f"trying the dialect: {dialect}")

                    connection_string = (
                        " Driver={%s}" % dialect
                        + "; SERVER=%s" % server
                        + "; Database=%s " % db
                        + "; UID=%s" % uid
                        + "; PWD=%s" % pwd
                    )

                    quoted = urllib.parse.quote_plus(connection_string)
                    quoted = f_data[accessType]["dialect_driver"] + quoted
                    # engine = create_engine(quoted, fast_executemany=True).execution_options(isolation_level="AUTOCOMMIT")
                    engine = create_engine(quoted, fast_executemany=True, pool_recycle = 3600, pool_pre_ping=True)
                    print(f"engine created with dialect = {dialect}")
                    try:
                        with engine.begin() as conn:
                            df = pd.DataFrame([1], columns=["test"])
                            df.to_sql(
                                "connectionTestTable",
                                conn,
                                if_exists="replace",
                                index=False,
                            )
                            print(f"engine test sucessful")
                            break
                    except:
                        print(f"the dialect = {dialect} didn't work")
            if accessType == "millenniumAccess":
                if dialect in f_data[accessType]["list_workingDialects"]:
                    print(f"trying the dialect: {dialect}")

                    connection_string = (
                        " Driver={%s}" % dialect
                        + "; SERVER=%s" % server
                        + "; Database=%s " % db
                        + "; UID=%s" % uid
                        + "; PWD=%s" % pwd
                        + "; Encrypt=no"
                        + "; Mars_Connection=yes"
                    )

                    quoted = urllib.parse.quote_plus(connection_string)
                    quoted = f_data[accessType]["dialect_driver"] + quoted
                    # engine = create_engine(quoted, fast_executemany=True).execution_options(isolation_level="AUTOCOMMIT")
                    engine = create_engine(quoted, fast_executemany=True)
                    print(f"engine created with dialect = {dialect}")
            else:
                print(driver + uid + ":" + pwd + "@" + server + ":" + str(port) + "/" + db)
                quoted = (
                    driver + uid + ":" + pwd + "@" + server + ":" + str(port) + "/" + db
                )
                engine = create_engine(quoted).execution_options(
                    isolation_level="AUTOCOMMIT"
                )
            str_error = None

        except:
            print("exception found, trying other dialect")
            pass
    return engine


# %%
def setupAPIrequest(utilities, extraParams = None, bodydata = None):
    """
    utilities: the utilies file
    extraParams: extraParams as Dictionary for adding params in the request
    bodydata: as dictionary
    """
    schemeHTTP = utilities["HTTP"]["schemeHTTP"]
    baseHTTP = utilities["HTTP"]["baseHTTP"]
    extraHTTP = utilities["HTTP"]["extraHTTP"]
    headers = utilities["HTTP"]["headers"]

    # adds default headers
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"

    # check if there is params variables:
    paramsHTTP = ""
    for key, value in utilities["HTTP"].items():
        if key == "params":
            for key, value in utilities["HTTP"]["params"].items():
                paramsHTTP = paramsHTTP + key + "=" + str(value) + "&"
            paramsHTTP = "?" + paramsHTTP
    if extraParams != None:
        for key, value in extraParams.items():
            paramsHTTP = paramsHTTP + key + "=" + str(value) + "&"
        paramsHTTP = paramsHTTP[:-1]
    completeHTTP = schemeHTTP + baseHTTP + extraHTTP + paramsHTTP

    if utilities["HTTP"]["method"] == "get":
        response = requests.get(completeHTTP, headers=headers)
    if utilities["HTTP"]["method"] == "post":
        response = requests.post(completeHTTP, headers=headers, data=bodydata)

    return response


# %%
def executeSQL(engine, sql_text):
    """
    gets an connection and a SQL code to run on the engine
    Returns the result query
    """
    conn = engine.connect()
    query_answer = conn.execute(sql_text)
    keys = query_answer.keys()

    answer = []
    for row in query_answer:
        n_coluna = 0
        mid_answer = {}
        for key in keys:
            mid_answer[key] = row[n_coluna]
            n_coluna += 1
        answer += [mid_answer]
    conn.close()
    
    return answer


# %%
def errorHandle(errSeverity, errReason, additionalInfo, file, engine_azure):
    """
    Handles error for logging in AzureDB:
    errLocation should be: where is running, application that is running + file name, other info
    errDescription should be: what went wrong probably
    errProcedure should be: how to restart/check the schedule or other info + if it's ok to retry anytime
    errSeverity: 1 to 5, where 1 is wait for next try and 5 is check immediately
    the connection is the connection for the AzureDB
    """
    print("started errorHandle")

    errProcedure = globals()["util"]["errorSuggestedProcedure"][errReason]
    if additionalInfo != None:
        errDescription = globals()["util"]["errorDescription"][errReason]
    else:
        errDescription = additionalInfo

    errLocation = globals()["util"][file]["nfo"]["runLocation"]
    errRunFileName = globals()["util"][file]["nfo"]["runFileName"]
    errRetry = globals()["util"][file]["nfo"]["retryOption"]

    globals()["endTime"] = datetime.now()
    timeDifference = globals()["endTime"] - globals()["startTime"]
    sql_text = f"""
        INSERT INTO nfo_errorLogTable (errorDescription, errorProcedure, errorStartTime, errorLocation, errorRetry, errorDuration, errorSeverity)
        VALUES ('{errDescription}', '{errProcedure}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{errLocation}: {errRunFileName}', '{errRetry}', {timeDifference.total_seconds()}, {errSeverity}) 
    """
    # tabela = Table('nfo_errorLogTable', MetaData(), autoload_with=engine_azure)
    # query = sa.insert(tabela).values(errorDescription = errDescription, errorProcedure = errProcedure, errorTime = datetime.now().strftime("%d/%m/%Y, %H:%M"), errorLocation = errLocation, errorSeverity = errSeverity)

    with engine_azure.begin() as conn:
        conn.execute(sql_text)


# %%
def successHandle(file, additionalInfo, runRowNumber, engine_azure):
    """
    Input information on function run success in AzureDB:
    :runFile: varchar(100) - describes the filename -> wms_function_vEstoqueConsulta.py
    :runStartTime: datetime - describes the startTime
    :runQueryName: varchar(100) - describes the queryName -> vEstoqueConsulta
    :runInputLocation: varchar(100) - describes the location of the input -> WMS_API
    :runOutputTable: varchar(100) - describes the Success outputTable in AzureDB -> wms_vEstoqueConsultaSuccess
    :runLocation: varchar(100) - describes where the pipeline is running -> AWS_batch
    :runDuration: datetime(100) - describes the run duration in seconds
    :additionalInfo: varchar(100) - additional information, optional
    :runRowNumber: (bigint) - describes how many rows were inserted in the table
    :engine_azure: is the azureDB defined engine
    """
    print("started successHandle")
    runFile = globals()["util"][file]["nfo"]["runFileName"]
    runQueryName = globals()["util"][file]["nfo"]["runQueryName"]
    runInputLocation = globals()["util"][file]["nfo"]["runInputLocation"]
    runOutputTable = globals()["util"][file]["nfo"]["runOutputSuccessTable"]
    runLocation = globals()["util"][file]["nfo"]["runLocation"]

    globals()["endTime"] = datetime.now()
    timeDifference = globals()["endTime"] - globals()["startTime"]

    # comes with insertion
    mainInsertionTimeDifference = globals()["mainEndTime"] - globals()["mainInsertTime"]

    # should be changed to attention Len instead of time
    globals()["attentionInsertTime"] = datetime.now()
    globals()["attentionEndTime"] = datetime.now()
    attentionInsertionTimeDifference = (
        globals()["attentionEndTime"] - globals()["attentionInsertTime"]
    )

    sql_text = f"""
        INSERT INTO nfo_successRunTable (runFile, runStartTime, runQueryName, runInputLocation, runOutputTable, runLocation, runDuration, runRowNumber, mainInsertionTimeDifference, attentionInsertionTimeDifference, additionalInfo)
        VALUES ('{runFile}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{runQueryName}', '{runInputLocation}', '{runOutputTable}', '{runLocation}', '{timeDifference.total_seconds()}', {runRowNumber}, {mainInsertionTimeDifference.total_seconds()}, {attentionInsertionTimeDifference.total_seconds()} ,'{additionalInfo}') 
    """
    if globals()["util"][file]["nfo"]["hasIdentifier"] == "y":
        sql_text = f"""
        INSERT INTO nfo_successRunTable (runFile, runStartTime, runQueryName, runInputLocation, runOutputTable, runLocation, runDuration, runRowNumber, mainInsertionTimeDifference, attentionInsertionTimeDifference, additionalInfo, identifier, identifierValue)
        VALUES ('{runFile}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{runQueryName}', '{runInputLocation}', '{runOutputTable}', '{runLocation}', '{timeDifference.total_seconds()}', {runRowNumber}, {mainInsertionTimeDifference.total_seconds()}, {attentionInsertionTimeDifference.total_seconds()} ,'{additionalInfo}', 
        '{globals()['util'][file]["nfo"]["identifier"]}' ,{globals()["max_identifiervalue"]}) 
        """

    with engine_azure.begin() as conn:
        conn.execute(sql_text)


# %%
def attentionHandle(file, additionalInfo, runRowNumber, engine_azure):
    """
    Input information on function run success in AzureDB:
    :runFile: varchar(100) - describes the filename -> wms_function_vEstoqueConsulta.py
    :runStartTime: datetime - describes the startTime
    :runQueryName: varchar(100) - describes the queryName -> vEstoqueConsulta
    :runInputLocation: varchar(100) - describes the location of the input -> WMS_API
    :runOutputTable: varchar(100) - describes the attention outputTable in AzureDB -> wms_vEstoqueConsultaAttention
    :runLocation: varchar(100) - describes where the pipeline is running -> AWS_batch
    :runDuration: datetime(100) - describes the run duration in seconds
    :additionalInfo: varchar(100) - additional information, optional
    :runRowNumber: (bigint) - describes how many rows were inserted in the table
    :engine_azure: is the azureDB defined engine
    """
    print("started attentionhandle")
    runFile = globals()["util"][file]["nfo"]["runFileName"]
    runQueryName = globals()["util"][file]["nfo"]["runQueryName"]
    runInputLocation = globals()["util"][file]["nfo"]["runInputLocation"]
    runOutputTable = globals()["util"][file]["resultSuccessTable"][file]
    runLocation = globals()["util"][file]["nfo"]["runLocation"]
    timeDifference = globals()["endTime"] - globals()["startTime"]
    mainInsertionTimeDifference = globals()["mainEndTime"] - globals()["mainInsertTime"]
    attentionInsertionTimeDifference = (
        globals()["attentionEndTime"] - globals()["attentionInsertTime"]
    )
    sql_text = f"""
        INSERT INTO nfo_attentionTable (runFile, runStartTime, runQueryName, runInputLocation, runOutputTable, runLocation, runDuration, runRowNumber, mainInsertionTimeDifference, attentionInsertionTimeDifference, additionalInfo)
        VALUES ('{runFile}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{runInputLocation}', '{runQueryName}', '{runOutputTable}', '{runLocation}', '{timeDifference.total_seconds()}', {runRowNumber} , {mainInsertionTimeDifference.total_seconds()}, {attentionInsertionTimeDifference.total_seconds()},'{additionalInfo}') 
    """
    with engine_azure.begin() as conn:
        conn.execute(sql_text)


# %%
def fCorrectTypes(dataFrame, columnsTypes_dict, list_dfAttention):
    '''
    gets a normalized data frame and a list of columns in a dictionary to change column type on the dataFrame
    returns a list_dfAttention a list with datetime errors, dataframe with the altered columns 
    '''
    for column in dataFrame:
        for key, value in columnsTypes_dict.items():
            if column == key:
                data_type = value["type"]
                data_format = value["format"]
                #copy the df to errDataTime
                errDataFrame = dataFrame

                #remove empty column cells
                errDataFrame = errDataFrame[errDataFrame[column].astype(bool)]
                #reindex the errDateTime to match with mask
                errDataFrame.reset_index(drop=True, inplace=True)
                
                #create a mask where the convertion to datetime fails
                if data_type == "to_datetime":
                    mask = pd.to_datetime(errDataFrame[column], format=data_format, errors='coerce').isna()
                if data_type == "to_numeric":
                    mask = pd.to_numeric(errDataFrame[column], errors='coerce').isna()

                #apply to df the mask from the substitution
                errDataFrame = errDataFrame[mask]

                #reindex the errDatetime
                errDataFrame.reset_index(drop=True, inplace=True)

                #append dataframe to be concatenated after only if there is > 1 row in the df
                if len(errDataFrame) > 0:
                    list_dfAttention.append(errDataFrame)

                #the main Dataframe is kept with all the data (and the errors are coerced)
                if data_type ==  "to_datetime":
                    dataFrame[column].fillna("", inplace=True)
                    dataFrame[column] = pd.to_datetime(dataFrame[column], format=data_format, errors="coerce")
                    dataFrame[column] = dataFrame[column].dt.tz_localize(None)
                if data_type == "to_numeric":
                    dataFrame[column].fillna(0, inplace=True)
                    #remove commas in case the numbers are stored as string
                    dataFrame[column] = dataFrame[column].replace(regex = {'[^0-9]', ''})
                    dataFrame[column] = dataFrame[column].replace(regex = {',', '.'})
                    #change dType
                    dataFrame[column] = pd.to_numeric(dataFrame[column], errors='coerce')
                break
        if dataFrame[column].dtype == int or dataFrame[column].dtype == float :
            dataFrame[column].fillna(0, inplace=True)
        else:
            dataFrame[column].fillna(np.NaN, inplace=True)
    return dataFrame, list_dfAttention

# %%
def df_typecheck(dfparam):
    '''
    gets an pandas dataframe, checks its contents and returns a dictionary of the types of the columns
    '''
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sa.types.VARCHAR(length=255)})

        if "datetime" in str(j):
            dtypedict.update({i: sa.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sa.types.Float(precision=3, asdecimal=False)})

        if "int" in str(j):
            dtypedict.update({i: sa.types.INT()})

    return dtypedict


# %%
class c_filial(object):
    def __init__(self, numfilial):
        self.numfilial = numfilial
        self.list_barras = []
        self.list_barras_srt = []
        
class c_barra(object):
    def __init__(self,  codbarra=None):
        self.codbarra = codbarra
        self.list_codnota = []
        self.list_codnota_str = []

class c_codnota(object):
    def __init__(self,data_origem=None, numfilial=None, data_movimento=None, data_nf=None, quantity=0, price=0, valor_total=0, numnota=None, custo_medio=0, codproduto=None, produto=None, codcor=None, codtamanho=None, codfilial=None, codbarra=None):
        self.codfilial = codfilial
        self.numfilial = numfilial
        self.codbarra = codbarra
        self.data_origem = data_origem
        self.data_movimento = data_movimento
        self.data_nf = data_nf
        self.price = price
        self.valor_total = valor_total
        self.codnota = numnota
        self.custo_medio = custo_medio
        self.codproduto = codproduto
        self.produto = produto
        self.codcor = codcor
        self.codtamanho = codtamanho

        self.quantity = quantity
        self.total_sold_quantity = None
        self.outbound_quantity = None

        self.fufilled_quantity = 0
        self.status = 'e'
        #p = partial
        #f = fullied
        #n = inbound greater than outbound

        self.last_nota_status = 'f'
        #t = true
        #f = false

class idworks_transaction(object):
    def __init__(self, numfilial, codbarra, quantity, transaction_code):
        self.numfilial = numfilial
        self.codbarra = codbarra
        self.quantity = quantity
        self.transaction_code = transaction_code

    def add_to_quantity(self, value_to_add):
        self.quantity = self.quantity + value_to_add


def f_check_barra (filial, codbarra):
    if codbarra not in filial.list_barras_srt:
        dummybarra = c_barra(codbarra)
        filial.list_barras += [dummybarra]
        filial.list_barras_srt += [codbarra]
        found = False
        found_barra = dummybarra
    else:
        for barra in filial.list_barras:
            if barra.codbarra == codbarra:
                #achou o produto
                found = True
                found_barra = barra
                break
    return found, found_barra

def f_check_nota (barra, numfilial, data_origem, data_movimento, data_nf, quantidade, preco, valor_total, numnota, custo_medio, codproduto, produto, codcor, codtamanho, codfilial, codbarra):
    if numnota not in barra.list_codnota_str:
        dummynota = c_codnota(data_origem, numfilial, data_movimento, data_nf, quantidade, preco, valor_total, numnota, custo_medio, codproduto, produto, codcor, codtamanho, codfilial, codbarra)
        barra.list_codnota += [dummynota]
        barra.list_codnota_str += [numnota]
        found = False
        found_nota = dummynota
    else:
        for nota in barra.list_codnota:
            if nota.codnota == numnota:
                found = True
                found_nota = nota
                break
    return found, found_nota

# %%
def df_removeLists(data_frame):
    """
    Removes the columns that contains lists in the data_frame
    returns the data_frame
    warning: the column data is lost
    Parameters
    ----------
    data_frame : pandas.DataFrame
        The DataFrame to be upserted.
    """
    #dfmask = ((data_frame.map(type) == list).all())
    #print (dfmask)
    #list_mask = (dfmask.mask(dfmask == False).dropna().index.to_list())
    #print (list_mask)
    list_mask = data_frame.columns[data_frame.applymap(lambda x: isinstance(x, list)).any()].tolist()
    return data_frame.drop(columns=list_mask)


# %%
def main(file):
    # open auth file for azureDB
    auth = open("auth.json")
    auth_load = json.load(auth)
    globals()["auth"] = auth_load

    # get utilities content
    util = open("utilities.json")
    utilities_load = json.load(util)
    globals()["util"] = utilities_load

    # create AzureDB connection
    engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    conn_azure = engine_azure.connect()

    #create millenniumDB connection
    engine_mill = getConnforMYSQL(auth_load, "millenniumAccess")
    conn_mill = engine_mill.connect()

    #make a list of filiais, objetos and notas
    globals()['list_filials'] = []
    list_numfiliais_str = []
    globals()['list_idworks_transactions'] = []
    list_idworks_transactions_code_str = []
    list_idworks_transactions_numfilial_str = []
    globals()['list_obj_notas'] = []

    #get idworksXmillennium accountname equivalencies
    list_filiais_idworks = executeSQL(engine_azure, sql_list["get_idworksxmillennium"])

    #build idworks auth dict
    bodypayload = '{"email":"' + globals()["auth"]["idworksAccess"]["email"] + '","password":"' + globals()["auth"]["idworksAccess"]["password"] +  '"}'

    #create idworks connection/tokens
    response = setupAPIrequest(globals()["util"]["idworks_signin_function"], None, bodypayload)
    idworks_response = json.loads(response.text)
    idworks_token = idworks_response['token']
    
    globals()["util"]["idworks_inboundquery_function"]["HTTP"]["headers"]["Authorization"] += f'{idworks_token}'
    globals()["util"]["idworks_outboundquery_function"]["HTTP"]["headers"]["Authorization"] += f'{idworks_token}'
    last_page = False
    page_counter = 1

    #get inbounds for idworks
    while last_page == False:
        extraparams = {"Page" : f"{page_counter}"}
        response = setupAPIrequest(globals()["util"]["idworks_inboundquery_function"], extraparams, None)
        idworks_response = json.loads(response.text)
        response_qnty = len(idworks_response)
        if response_qnty == 0:
            last_page = True
        else:
            #create obj_nota for each nota
            for each_idworks_nota in idworks_response:
                found = False
                for item in list_filiais_idworks:
                    if item['accountname_idworks'] == each_idworks_nota['AccountName']:
                        idworks_numfilial = item['numFilial_millennium']

                        #define necessary values
                        data_origem = 'idworks'
                        if each_idworks_nota['RecordTimestamp'] != None:
                            data_movimento = each_idworks_nota['RecordTimestamp'].split('Z')[0]
                        else:
                            data_movimento = '0000-00-00T00:00:00.000'
                        if each_idworks_nota['NfeDhEmi'] != None:
                            data_nf = each_idworks_nota['NfeDhEmi'].split('Z')[0]
                        else:
                            data_nf = '0000-00-00T00:00:00.000'
                        numnota = each_idworks_nota['NfeNumber']
                        codbarra = each_idworks_nota['BarCode']
                        codproduto = each_idworks_nota['IDSkuCompany']
                        produto = each_idworks_nota['IDSku']
                        codcor = None
                        codtamanho = None
                        quantidade = float(each_idworks_nota['Quantity'])
                        preco = (each_idworks_nota['NfevProd'])
                        valor_total = (each_idworks_nota['ValueCostTotal'])
                        custo_medio = (each_idworks_nota['ValueCost'])
                        codfilial = each_idworks_nota['AccountName']

                        #check if dummy filial is in the list
                        if idworks_numfilial not in list_numfiliais_str:
                            #create the dummy filial and put in the list
                            dummyfilial = c_filial(idworks_numfilial)
                            list_numfiliais_str += [idworks_numfilial]
                            globals()['list_filials'] += [dummyfilial]
                            found_filial = dummyfilial
                        else:
                            for each_filial in globals()['list_filials']:
                                if each_filial.numfilial == idworks_numfilial:
                                    found_filial = each_filial
                                    break
                            
                        #create barra and nota
                        check_barra, barra = f_check_barra(found_filial, codbarra)
                        check_nota, nota = f_check_nota(barra, idworks_numfilial, data_origem, data_movimento, data_nf, quantidade, preco, valor_total, numnota, custo_medio, codproduto, produto, codcor, codtamanho, codfilial, codbarra)
                        found = True
                if found == False:
                    print (f"account: {each_idworks_nota['AccountName']} is not registered in azureDB nfo_idworksxmillennium table")
        page_counter += 1
    print ('finished processing idworks inbounds')

    #get outbounds idworks
    last_page = False
    page_counter = 1

    while last_page == False:
        extraparams = {"Page" : f"{page_counter}"}
        response = setupAPIrequest(globals()["util"]["idworks_outboundquery_function"], extraparams, None)
        idworks_response = json.loads(response.text)
        response_qnty = len(idworks_response)
        if response_qnty == 0:
            last_page = True
        else:
            #create idworks_outbound_product for each product in idworks
            for each_idworks_transaction in idworks_response:
                found = False
                for item in list_filiais_idworks:
                    if item['accountname_idworks'] == each_idworks_transaction['AccountName']:
                        idworks_numfilial = item['numFilial_millennium']

                        if idworks_numfilial not in list_idworks_transactions_numfilial_str:
                            list_idworks_transactions_numfilial_str += [idworks_numfilial]

                        #define necessary values
                        codbarra = each_idworks_transaction['BarCode']
                        quantity = float(each_idworks_transaction['Quantity'])

                        idworks_transaction_code = str(idworks_numfilial) + str(codbarra)

                        #check if dummy filial is in the list
                        if idworks_transaction_code not in list_idworks_transactions_code_str:
                            dummytransaction = idworks_transaction(idworks_numfilial, codbarra, quantity, idworks_transaction_code)
                            list_idworks_transactions_code_str += [idworks_transaction_code]

                            #globals()['list_idworks_transactions'] += [dummytransaction]
                        else:
                            for each_transaction in globals()['list_idworks_transactions']:
                                if each_transaction.transaction_code == idworks_transaction_code:
                                    each_transaction.add_to_quantity(quantity)
                                    break
            page_counter += 1
    print ('finished processing idworks outbounds')

    # list_dict_filiais = executeSQL(engine_mill, sql_list["getFiliaisList"])
    # # list_dict_filiais = [{'numfilial':21}]
    
    # for item in list_dict_filiais:
    #     numfilial = item['numfilial']
        
    #     #check if filial already in globals["list_filials"] list and find the filial object
    #     if numfilial not in list_numfiliais_str:
    #         dummyfilial = c_filial(numfilial)
    #         list_numfiliais_str += [numfilial]
    #         print (f'created filial {numfilial}')
    #         globals()["list_filials"] += [dummyfilial]
    
    # new_list = []
    # for item in globals()['list_filials']:
    #     if item.numfilial == 21:
    #         new_list += [item]

    # globals()["list_filials"] = new_list
    
    for item in globals()['list_filials']:
        found_filial = item
        numfilial = item.numfilial
        #reset values
        globals()['list_obj_notas'] = []
        df = None
        
        print (f'found_filial :{found_filial.numfilial}')

        #get the products of the filial
        list_produtos = executeSQL(engine_mill, sql_list["get_entradas_by_numero"].format(numerofilial = numfilial))
        print (f'len {len(list_produtos)} list_produtos for filial {numfilial}')
        
        for each_row in list_produtos:
            data_origem = 'millennium'
            data_movimento = each_row['data_movimento']
            data_nf = each_row['data_nf']
            numnota = each_row['nota']
            codbarra = each_row['codbarra']
            codproduto = each_row['codproduto']
            produto = each_row['produto']
            codcor = each_row['codcor']
            codtamanho = each_row['codtamanho']
            quantidade = each_row['quantidade']
            preco = each_row['preco']
            valor_total = each_row['valor_total']
            custo_medio = round(each_row['custo_medio'],2)
            codfilial = each_row['codfilial']
            
            check_barra, barra = f_check_barra(found_filial, codbarra)
            check_nota, nota = f_check_nota(barra, numfilial, data_origem, data_movimento, data_nf, quantidade, preco, valor_total, numnota, custo_medio, codproduto, produto, codcor, codtamanho, codfilial, codbarra)

        for each_barra in found_filial.list_barras:
            codbarra = each_barra.codbarra
            print (codbarra)

            #sort by data_movimento
            each_barra.list_codnota = sorted(each_barra.list_codnota, key = lambda nota: nota.data_movimento)

            #search in idworks_transactions
            idworks_search_code = str(numfilial) + str(codbarra)
            idworks_quantity = 0
            if idworks_search_code in list_idworks_transactions_code_str:
                for each_idworks_transaction in globals()['list_idworks_transactions']:
                    if each_idworks_transaction.numfilial == numfilial and each_idworks_transaction.codbarra == codbarra:
                        idworks_quantity = each_idworks_transaction.quantity
                        print (f'found quantity in idworks: {idworks_quantity}')
                    
            #gets data from outbound of that filial, barra
            list_outbound = executeSQL(engine_mill, sql_list['get_saidas'].format(numerofilial = numfilial, codbarra = codbarra ))
            
            if len(list_outbound) > 1:
                print('something went wrong, more than one item in list_outbound')
            else:
                if len(list_outbound) == 0:
                    mill_quantity = 0
                else:
                    mill_quantity = list_outbound[0]['quantidade']

            if mill_quantity + idworks_quantity == 0:
                print(f'codbarra: {codbarra} status: no outbound product')
                outbound_number = 0
                for each_nota in each_barra.list_codnota:
                    each_nota.total_sold_quantity = 0
                    each_nota.status = 'x'
                    outbound_number = outbound_number - each_nota.quantity
                    each_nota.outbound_quantity = outbound_number
                    last_nota = each_nota
                last_nota.last_nota_status = 't'
            else:
                outbound_number = mill_quantity + idworks_quantity
                print(f'codbarra: {codbarra} status: found outbound {outbound_number}')
                total_sold_qnty = outbound_number
                last_date = each_barra.list_codnota[0].data_movimento
                for each_nota in each_barra.list_codnota:
                    #save total sold quantity
                    each_nota.total_sold_quantity = total_sold_qnty

                    #ensure the current date is after the last date
                    curr_date = each_nota.data_movimento
                    if last_date <= curr_date:
                        curr_qnty = each_nota.quantity

                        if outbound_number == 0:
                            each_nota.fufilled_quantity = 0
                            outbound_number = outbound_number - curr_qnty
                            each_nota.status = 'e'
                        elif outbound_number < 0:
                            each_nota.fufilled_quantity = 0
                            outbound_number = outbound_number - curr_qnty
                            each_nota.status = 'n'
                        elif outbound_number >= curr_qnty:
                            each_nota.fufilled_quantity = curr_qnty
                            outbound_number = outbound_number - curr_qnty
                            each_nota.status = 'f'
                        elif outbound_number < curr_qnty:
                            each_nota.fufilled_quantity = curr_qnty
                            outbound_number = outbound_number - curr_qnty
                            each_nota.status = 'p'                           
                    else:
                        print('something is wrong with the dates of the notas')
                        print(f'last_date: {last_date}')
                        print(f'curr_date: {curr_date}')
                    each_nota.outbound_quantity = outbound_number
                    print (f"barra: {codbarra}, date: {each_nota.data_movimento}, codnota: {each_nota.codnota}, status: {each_nota.status}, fuqnty: {each_nota.fufilled_quantity}, qnty: {each_nota.quantity}, sold_qnty: {each_nota.total_sold_quantity},  curr_outbound_number: {outbound_number}")    

                    last_date = curr_date
                    last_nota = each_nota
                last_nota.last_nota_status = 't'
        
        for each_barra in found_filial.list_barras:
            for each_nota in each_barra.list_codnota:
                globals()['list_obj_notas'] += [vars(each_nota)]


        list_dfAttention = []
        
        df = pd.DataFrame.from_records(globals()['list_obj_notas'])
        df, list_dfAttention = fCorrectTypes(df, globals()['util']["dataprocessing_fechamentonfs"], list_dfAttention)
        df = df_removeLists(df)
        df = df.drop_duplicates()

        df_dtypes = df_typecheck(df)
        df.to_sql(utilities_load['dataprocessing_fechamentonfs']["resultSuccessTable"]["dataprocessing_fechamentonfs"], engine_azure, if_exists='append', index=False, dtype = df_dtypes)


    # for each_filial in globals()['list_filials'] :
    #     for each_barra in each_filial.list_barras:
    #         for each_nota in each_barra.list_codnota:
    #            globals()['list_obj_notas'] += [vars(each_nota)]


    # list_dfAttention = []
    
    # df = pd.DataFrame.from_records(globals()['list_obj_notas'])
    # df, list_dfAttention = fCorrectTypes(df, globals()['util']["dataprocessing_fechamentonfs"], list_dfAttention)
    # df = df_removeLists(df)
    # df = df.drop_duplicates()

    # df_dtypes = df_typecheck(df)
    # df.to_sql(utilities_load['dataprocessing_fechamentonfs']["resultSuccessTable"]["dataprocessing_fechamentonfs"], engine_azure, if_exists='replace', index=False, dtype = df_dtypes)


# %%
if __name__ == "__main__":
    file = "dataprocessing_fechamentonfs"
    print(f'{file} start time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    globals()["startTime"] = datetime.now()

    main(file)
    globals()["endTime"] = datetime.now()
    print(
        "%s: done with the output: %s, runtime %s"
        % (
            file,
            globals()["output"],
            (globals()["endTime"] - globals()["startTime"]).total_seconds(),
        )
    )





