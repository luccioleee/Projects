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
                    engine = create_engine(quoted, fast_executemany=True)
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
def setupAPIrequest(utilities, extraParams):
    """
    utilities: the utilies file
    extraParams: extraParams as Dictionary for adding params in the request
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
    if extraParams != "":
        for key, value in extraParams.items():
            paramsHTTP = paramsHTTP + key + "=" + str(value) + "&"
        paramsHTTP = paramsHTTP[:-1]
    completeHTTP = schemeHTTP + baseHTTP + extraHTTP + paramsHTTP

    if utilities["HTTP"]["method"] == "get":
        response = requests.get(completeHTTP, headers=headers)

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
    """
    gets a normalized data frame and a list of columns in a dictionary to change column type on the dataFrame
    returns a list_dfAttention a list with datetime errors, dataframe with the altered columns
    """
    for column in dataFrame:
        for key, value in columnsTypes_dict.items():
            if column == key:
                data_type = value["type"]
                data_format = value["format"]
                # copy the df to errDataTime
                errDataFrame = dataFrame

                # remove empty column cells
                errDataFrame = errDataFrame[errDataFrame[column].astype(bool)]
                # reindex the errDateTime to match with mask
                errDataFrame.reset_index(drop=True, inplace=True)

                # create a mask where the convertion to datetime fails
                if data_type == "to_datetime":
                    mask = pd.to_datetime(
                        errDataFrame[column], format=data_format, errors="coerce"
                    ).isna()
                if data_type == "to_numeric":
                    mask = pd.to_numeric(errDataFrame[column], errors="coerce").isna()

                # apply to df the mask from the substitution
                errDataFrame = errDataFrame[mask]

                # reindex the errDatetime
                errDataFrame.reset_index(drop=True, inplace=True)

                # append dataframe to be concatenated after only if there is > 1 row in the df
                if len(errDataFrame) > 0:
                    list_dfAttention.append(errDataFrame)

                # the main Dataframe is kept with all the data (and the errors are coerced)
                if data_type == "to_datetime":
                    dataFrame[column].fillna("", inplace=True)
                    dataFrame[column] = pd.to_datetime(
                        dataFrame[column], format=data_format, errors="coerce"
                    )
                if data_type == "to_numeric":
                    dataFrame[column].fillna(0, inplace=True)
                    # remove commas in case the numbers are stored as string
                    dataFrame[column] = dataFrame[column].replace(regex={"[^0-9]", ""})
                    dataFrame[column] = dataFrame[column].replace(regex={",", "."})
                    # change dType
                    dataFrame[column] = pd.to_numeric(
                        dataFrame[column], errors="coerce"
                    )
                break
        if dataFrame[column].dtype == int or dataFrame[column].dtype == float:
            dataFrame[column].fillna(0, inplace=True)
        else:
            dataFrame[column].fillna("", inplace=True)
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
            dtypedict.update({i: sa.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sa.types.INT()})

    return dtypedict


# %%
class c_filial(object):
    def __init__(self, numfilial):
        self.numfilial = numfilial
        self.list_products = []
        
class c_product(object):
    def __init__(self,  codproduct=None):
        self.codproduct = codproduct
        self.list_codcolour = []
        
class c_codcolour(object):
    def __init__(self, codcolour=None):
        self.codcolour = codcolour
        self.list_codsize = []

class c_codsize(object):
    def __init__(self, date=None, codsize=None, quantity=None, price=None, nota=None, custo_medio=None):
        self.date = date
        self.codsize = codsize
        self.quantity = quantity
        self.price = price
        self.nota = nota
        self.custo_medio = custo_medio

        self.fufilled_quantity = None
        self.status = 'e'
        #p = partial
        #f = fullied
        #e = empty


# %%
class Thread_gatherdata (object):
    def __init__(self, file, engine):
        self.engine = engine

        #start the thread
        self.t = Thread(target=self.Thread_gatherdataMainTask, args=())
        self.t.start()

    def getThread(self):
        return (self.t)

    def Thread_gatherdataMainTask(self):
        list_produtos = executeSQL(self.engine, sql_list["get_entradas"])
        for each_row in list_produtos:
            numfilial = each_row['filial']
            data = each_row['data']
            nota = each_row['nota']
            codproduto = each_row['codproduto']
            produto = each_row['produto']
            codcor = each_row['codcor']
            codtamanho = each_row['codtamanho']
            quantidade = each_row['quantidade']
            preco = each_row['preco']
            custo_medio = each_row['custo_medio']

        
            if len(globals()["list_obj_filial"]) == 0 :
                dummyfilial = c_filial(numfilial)
                dummyproduct = c_product(codproduto)
                dummycolour = c_codcolour(codcor)
                dummysize = c_codsize(data, codtamanho, quantidade, preco, nota, custo_medio)

                dummycolour.list_codsize += [dummysize] 
                dummyproduct.list_codcolour  += [dummycolour]
                dummyfilial.list_products += [dummyproduct]

                globals()["list_obj_filial"] += [dummyfilial]
            else:
                for filial in globals()["list_obj_filial"]:
                    if filial.numfilial == numfilial:
                        for product in filial.list_products:
                            if product.codproduct == codproduto:
                                for colour in product.list_codcolour:
                                    if colour.codcolour == codcor:
                                        for size in colour.list_codsize:
                                            if size.codsize == codtamanho:
                                                break
                                            else:
                                                dummysize = c_codsize(data, codtamanho, quantidade, preco, nota, custo_medio)
                                                colour.list_codsize += [dummysize]
                                        break
                                    else:
                                        dummycolour = c_codcolour(codcor)
                                        dummysize = c_codsize(data, codtamanho, quantidade, preco, nota, custo_medio)

                                        dummycolour.list_codsize += [dummysize]
                                        product.list_codcolour += [dummycolour]
                                break
                            else:
                                #if product not in list, insert everything
                                dummyproduct = c_product(codproduto)
                                dummycolour = c_codcolour(codcor)
                                dummysize = c_codsize(data, codtamanho, quantidade, preco, nota, custo_medio)

                                dummycolour.list_codsize += [dummysize] 
                                dummyproduct.list_codcolour  += [dummycolour]
                                filial.list_products += [dummyproduct] 
                        break
                    else:
                        dummyfilial = c_filial(numfilial)
                        dummyproduct = c_product(codproduto)
                        dummycolour = c_codcolour(codcor)
                        dummysize = c_codsize(data, codtamanho, quantidade, preco, nota, custo_medio)

                        dummycolour.list_codsize += [dummysize] 
                        dummyproduct.list_codcolour  += [dummycolour]
                        dummyfilial.list_products += [dummyproduct]

                        globals()["list_obj_filial"] += [dummyfilial]

                        
                    print (len(filial.list_products))




# %%
def main(file):
    # open auth file for azureDB
    auth = open("auth.json")
    auth_load = json.load(auth)

    # create AzureDB connection
    engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    conn_azure = engine_azure.connect()

    #create millenniumDB connection
    engine_mill = getConnforMYSQL(auth_load, "millenniumAccess")
    conn_mill = engine_mill.connect()

    # get utilities content
    util = open("utilities.json")
    utilities_load = json.load(util)
    globals()["util"] = utilities_load

    #get distinct filials from millennium, make a thread for each of them
    globals()['list_filials'] = []
    threads = []
    list_dict_filiais = executeSQL(engine_mill, sql_list["getFiliaisList"])

    globals()["list_obj_filial"] = []

    t = Thread_gatherdata(file, engine_mill)
    threads.append(t.getThread())
    
    for t in threads:
        t.join()



# %%
if __name__ == "__main__":
    file = "datapipeline_millennium_recebimentos"
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





