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

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.sharepoint.folders.folder import Folder
import os

# %%
#gets connections for AzureDB
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
                    print (f"trying the dialect: {dialect}")

                    connection_string = (
                        " Driver={%s}" %dialect +
                        "; SERVER=%s" %server + 
                        "; Database=%s " %db + 
                        "; UID=%s" %uid +
                        "; PWD=%s" %pwd
                    )
                    
                    quoted = urllib.parse.quote_plus(connection_string)
                    quoted = f_data[accessType]["dialect_driver"] + quoted
                    #engine = create_engine(quoted, fast_executemany=True).execution_options(isolation_level="AUTOCOMMIT")
                    engine = create_engine(quoted, fast_executemany=True)
                    print (f"engine created with dialect = {dialect}")
                    try:
                        with engine.begin() as conn:
                            df = pd.DataFrame([1], columns = ['test'])
                            df.to_sql("connectionTestTable", conn, if_exists="replace", index = False)
                            print(f"engine test sucessful")
                            break
                    except:
                        print(f"the dialect = {dialect} didn't work")
            else:
                quoted = driver + uid + ":" + pwd + "@" + server + ":" + str(port) + "/" + db
                engine = create_engine(quoted).execution_options(isolation_level="AUTOCOMMIT")
            str_error = None

        except:
            print('exception found, trying other dialect')
            pass
    return engine

# %%
#get response from API
def setupAPIrequest(utilities, extraParams):
    '''
    utilities: the utilies file
    extraParams: extraParams as Dictionary for adding params in the request
    '''
    schemeHTTP = utilities["HTTP"]["schemeHTTP"]
    baseHTTP = utilities["HTTP"]["baseHTTP"]
    extraHTTP = utilities["HTTP"]["extraHTTP"]
    headers = utilities["HTTP"]["headers"]
    
    #adds default headers
    headers['Accept'] =  "application/json"
    headers['Content-Type'] =  "application/json"   

    #check if there is params variables:
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
#connect to sharepoint
def getConnForSharepoint (user, password, download_path, file_url, url):
    ctx_auth = AuthenticationContext(url)
    ctx_auth.acquire_token_for_user(user, password)   
    ctx = ClientContext(url, ctx_auth)
    
    with open(download_path, "wb") as local_file:
        file = ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()


# %%
def errorHandle(errSeverity, errReason, additionalInfo, file, engine_azure):
    '''
    Handles error for logging in AzureDB:
    errLocation should be: where is running, application that is running + file name, other info
    errDescription should be: what went wrong probably
    errProcedure should be: how to restart/check the schedule or other info + if it's ok to retry anytime
    errSeverity: 1 to 5, where 1 is wait for next try and 5 is check immediately
    the connection is the connection for the AzureDB
    '''
    print("started errorHandle")

    errProcedure = globals()['util']["errorSuggestedProcedure"][errReason]
    if additionalInfo != None:
        errDescription = globals()['util']["errorDescription"][errReason]
    else:
        errDescription = additionalInfo

    errLocation = globals()["util"][file]["nfo"]["runLocation"]
    errRunFileName = globals()["util"][file]["nfo"]["runFileName"]
    errRetry = globals()["util"][file]["nfo"]["retryOption"]

    globals()['endTime'] = datetime.now()
    timeDifference = (globals()['endTime'] - globals()['startTime'])
    sql_text = f"""
        INSERT INTO nfo_errorLogTable (errorDescription, errorProcedure, errorStartTime, errorLocation, errorRetry, errorDuration, errorSeverity)
        VALUES ('{errDescription}', '{errProcedure}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{errLocation}: {errRunFileName}', '{errRetry}', {timeDifference.total_seconds()}, {errSeverity}) 
    """
    #tabela = Table('nfo_errorLogTable', MetaData(), autoload_with=engine_azure)
    #query = sa.insert(tabela).values(errorDescription = errDescription, errorProcedure = errProcedure, errorTime = datetime.now().strftime("%d/%m/%Y, %H:%M"), errorLocation = errLocation, errorSeverity = errSeverity)
    
    with engine_azure.begin() as conn:
        conn.execute(sql_text)

# %%
def successHandle(file, additionalInfo, runRowNumber, engine_azure):
    '''
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
    '''
    print("started successHandle")
    runFile = globals()["util"][file]["nfo"]["runFileName"]
    runQueryName = globals()["util"][file]["nfo"]["runQueryName"]
    runInputLocation = globals()["util"][file]["nfo"]["runInputLocation"]
    runOutputTable = globals()["util"][file]["nfo"]["runOutputSuccessTable"]
    runLocation = globals()["util"][file]["nfo"]["runLocation"]

    globals()['endTime'] = datetime.now()
    timeDifference = (globals()['endTime'] - globals()['startTime'])

    #comes with insertion
    mainInsertionTimeDifference = (globals()['mainEndTime'] - globals()['mainInsertTime'])
    
    #should be changed to attention Len instead of time
    globals()['attentionInsertTime'] = datetime.now()
    globals()['attentionEndTime'] = datetime.now()
    attentionInsertionTimeDifference = (globals()['attentionEndTime'] - globals()['attentionInsertTime'])
    
    sql_text = f"""
        INSERT INTO nfo_successRunTable (runFile, runStartTime, runQueryName, runInputLocation, runOutputTable, runLocation, runDuration, runRowNumber, mainInsertionTimeDifference, attentionInsertionTimeDifference, additionalInfo)
        VALUES ('{runFile}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{runQueryName}', '{runInputLocation}', '{runOutputTable}', '{runLocation}', '{timeDifference.total_seconds()}', {runRowNumber}, {mainInsertionTimeDifference.total_seconds()}, {attentionInsertionTimeDifference.total_seconds()} ,'{additionalInfo}') 
    """
    if globals()['util'][file]["nfo"]["hasIdentifier"] == "y":
        sql_text = f"""
        INSERT INTO nfo_successRunTable (runFile, runStartTime, runQueryName, runInputLocation, runOutputTable, runLocation, runDuration, runRowNumber, mainInsertionTimeDifference, attentionInsertionTimeDifference, additionalInfo, identifier, identifierValue)
        VALUES ('{runFile}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{runQueryName}', '{runInputLocation}', '{runOutputTable}', '{runLocation}', '{timeDifference.total_seconds()}', {runRowNumber}, {mainInsertionTimeDifference.total_seconds()}, {attentionInsertionTimeDifference.total_seconds()} ,'{additionalInfo}', 
        '{globals()['util'][file]["nfo"]["identifier"]}' ,{globals()["max_identifiervalue"]}) 
        """

    with engine_azure.begin() as conn:
        conn.execute(sql_text)


# %%
def attentionHandle(file, additionalInfo, runRowNumber, engine_azure):
    '''
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
    '''
    print("started attentionhandle")
    runFile = globals()["util"][file]["nfo"]["runFileName"]
    runQueryName = globals()["util"][file]["nfo"]["runQueryName"]
    runInputLocation = globals()["util"][file]["nfo"]["runInputLocation"]
    runOutputTable = globals()["util"][file]["resultAttentionTable"][file]
    runLocation = globals()["util"][file]["nfo"]["runLocation"]
    
    timeDifference = (globals()['endTime'] - globals()['startTime'])
    mainInsertionTimeDifference = (globals()['mainEndTime'] - globals()['mainInsertTime'])
    attentionInsertionTimeDifference = (globals()['attentionEndTime'] - globals()['attentionInsertTime'])
    
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
                if data_type == "to_dateTime":
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
                if data_type ==  "to_dateTime":
                    dataFrame[column] = pd.to_datetime(dataFrame[column], format=data_format, errors="coerce")
                if data_type == "to_numeric" and not (dataFrame[column].dtype == int or dataFrame[column].dtype == float):
                    #remove commas in case the numbers are stored as string
                    dataFrame[column] = dataFrame[column].str.replace(',','.')
                    #change dType
                    dataFrame[column] = pd.to_numeric(dataFrame[column], errors='coerce')
                break
        if dataFrame[column].dtype == int or dataFrame[column].dtype == float :
            dataFrame[column].fillna(0, inplace=True)
        else:
            dataFrame[column].fillna("", inplace=True)
                
    
    return dataFrame, list_dfAttention

# %%
def main(file):
    #open auth file for azureDB
    auth = open('auth.json')
    auth_load = json.load(auth)
    
    #create AzureDB connection
    engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    conn_azure = engine_azure.connect()

    #get utilities content
    util = open('utilities.json')
    utilities_load = json.load(util)
    globals()['util'] = utilities_load

    list_dfAttention = []

    #download OneD files to this folder
    try:
        getConnForSharepoint(auth_load["OneDAccess"]["user"], auth_load["OneDAccess"]["password"], utilities_load[file]["HTTP"]["download_path"], utilities_load[file]["HTTP"]["file_url"], utilities_load[file]["HTTP"]["oneD_url"])
        print(f"Download from oneD Successful. Time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        #if the the download is OK, then
        try:
            #creating a dataframe with the excel file
            df_regrasGMV = pd.read_excel("./file.xlsx", sheet_name='RegraGMV')
            df_regrasOther = pd.read_excel("./file.xlsx", sheet_name='RegrasOther')
            df_filiais = pd.read_excel("./file.xlsx", sheet_name='filiais')
            df_nomenclaturaARMZ = pd.read_excel("./file.xlsx", sheet_name='nomenclaturaARMZ')
            df_valoresFormulario = pd.read_excel("./file.xlsx", sheet_name='valoresFormulario')
            df_regraMarkupTransportadoras = pd.read_excel("./file.xlsx", sheet_name='regraMarkupTransportadoras')
            df_faturaCorreios = pd.read_excel("./file.xlsx", sheet_name='faturaCorreios')

            #correct type
            df_regrasGMV, list_dfAttention = fCorrectTypes(df_regrasGMV, globals()['util'][file]["columnsType_dict"]["regrasGMV"] ,list_dfAttention)
            df_regrasOther, list_dfAttention = fCorrectTypes(df_regrasOther, globals()['util'][file]["columnsType_dict"]["regrasOther"] ,list_dfAttention)
            df_filiais, list_dfAttention = fCorrectTypes(df_filiais, globals()['util'][file]["columnsType_dict"]["filiais"] ,list_dfAttention)
            df_nomenclaturaARMZ, list_dfAttention = fCorrectTypes(df_nomenclaturaARMZ, globals()['util'][file]["columnsType_dict"]["nomenclaturaARMZ"] ,list_dfAttention)
            df_valoresFormulario, list_dfAttention = fCorrectTypes(df_valoresFormulario, globals()['util'][file]["columnsType_dict"]["valoresFormulario"] ,list_dfAttention)
            df_regraMarkupTransportadoras, list_dfAttention = fCorrectTypes(df_regraMarkupTransportadoras, globals()['util'][file]["columnsType_dict"]["regraMarkupTransportadoras"] ,list_dfAttention)
            df_faturaCorreios, list_dfAttention = fCorrectTypes(df_faturaCorreios, globals()['util'][file]["columnsType_dict"]["faturaCorreios"] ,list_dfAttention)

            try:
                #insert into AzureDB the main df
                print (f'{file} starting mainInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')

                globals()['mainInsertTime'] = datetime.now()
                df_regrasGMV.to_sql(utilities_load[file]["resultSuccessTable"]["regrasGMV"], engine_azure, if_exists='replace', index=False)
                df_regrasOther.to_sql(utilities_load[file]["resultSuccessTable"]["regrasOther"], engine_azure, if_exists='replace', index=False)
                df_filiais.to_sql(utilities_load[file]["resultSuccessTable"]["filiais"], engine_azure, if_exists='replace', index=False)
                df_nomenclaturaARMZ.to_sql(utilities_load[file]["resultSuccessTable"]["nomenclaturaARMZ"], engine_azure, if_exists='replace', index=False)
                df_valoresFormulario.to_sql(utilities_load[file]["resultSuccessTable"]["valoresFormulario"], engine_azure, if_exists='replace', index=False)
                df_regraMarkupTransportadoras.to_sql(utilities_load[file]["resultSuccessTable"]["regraMarkupTransportadoras"], engine_azure, if_exists='replace', index=False)
                df_faturaCorreios.to_sql(utilities_load[file]["resultSuccessTable"]["faturaCorreios"], engine_azure, if_exists='replace', index=False)
                globals()['mainEndTime'] = datetime.now()
                
                #mark clocks
                globals()['endTime'] = datetime.now()
                globals()['attentionInsertTime'] = datetime.now()
                globals()['attentionEndTime'] = datetime.now()

                #for the main DataFrame
                successHandle(file=file, additionalInfo= "", runRowNumber= (len(df_regrasGMV) + len(df_regrasOther) + len(df_filiais) + len(df_nomenclaturaARMZ) + len(df_valoresFormulario)), engine_azure = engine_azure)
                globals()['output'] = "Success"
            except:
                errorHandle(2, "InsertAzureDB", None, file, engine_azure)
        except:
            errorHandle(5, "FailedDataFrame", None, file, engine_azure)
    except:
        print(f"failed to download from oneD. Time: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        errorHandle(5, "OneD", None, "oneD_utilities", engine_azure)
            

# %%
def testRowNumber():
    '''
    to test the output of the function, first update example.json file in folder -> to do this, copy the output of the postman response in the dictionary
    then run this function and compare the number of rows between AzureDB and the dataframe
    '''
    result = open("example.json", 'r', encoding='utf-8')
    result = json.loads(result.read())

    df = pd.json_normalize(result['x'])
    print(df)

# %%
if __name__ == "__main__":
    file = "oneD_utilities"
    print (f'{file} start time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    globals()['startTime'] = datetime.now()
    globals()['output'] = "Failed"
    
    main(file)

    print('%s: done with the output: %s, runtime %s' %(file, globals()['output'], (globals()['endTime'] - globals()['startTime']).total_seconds()))


