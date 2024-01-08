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
def setupAPIrequest(utilities):
    schemeHTTP = utilities["schemeHTTP"]
    baseHTTP = utilities["baseHTTP"]
    extraHTTP = utilities["extraHTTP"]
    headers = utilities["headers"]
    
    #adds default headers
    headers['Accept'] =  "application/json"
    headers['Content-Type'] =  "application/json"   

    completeHTTP = schemeHTTP + baseHTTP + extraHTTP

    if utilities["method"] == "get":
        response = requests.get(completeHTTP, headers=headers)
    
    return response

# %%
def errorHandle(errSeverity, errProcedure, errDescription, file, engine_azure):
    '''
    Handles error for logging in AzureDB:
    errLocation should be: where is running, application that is running + file name, other info
    errDescription should be: what went wrong probably
    errProcedure should be: how to restart/check the schedule or other info + if it's ok to retry anytime
    errSeverity: 1 to 5, where 1 is wait for next try and 5 is check immediately
    the connection is the connection for the AzureDB
    '''
    print("started errorHandle")
    errLocation = globals()["util"][file]["runLocation"]
    errRunFileName = globals()["util"][file]["runFileName"]
    errRetry = globals()["util"][file]["retryOption"]

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
    runFile = globals()["util"][file]["runFileName"]
    runQueryName = globals()["util"][file]["runQueryName"]
    runInputLocation = globals()["util"][file]["runInputLocation"]
    runOutputTable = globals()["util"][file]["resultSuccessTable"]
    runLocation = globals()["util"][file]["runLocation"]
    timeDifference = (globals()['endTime'] - globals()['startTime'])
    mainInsertionTimeDifference = (globals()['mainEndTime'] - globals()['mainInsertTime'])
    attentionInsertionTimeDifference = (globals()['attentionEndTime'] - globals()['attentionInsertTime'])
    sql_text = f"""
        INSERT INTO nfo_successRunTable (runFile, runStartTime, runQueryName, runInputLocation, runOutputTable, runLocation, runDuration, runRowNumber, mainInsertionTimeDifference, attentionInsertionTimeDifference, additionalInfo)
        VALUES ('{runFile}', '{globals()['startTime'].strftime("%m/%d/%Y %H:%M")}', '{runQueryName}', '{runInputLocation}', '{runOutputTable}', '{runLocation}', '{timeDifference.total_seconds()}', {runRowNumber}, {mainInsertionTimeDifference.total_seconds()}, {attentionInsertionTimeDifference.total_seconds()} ,'{additionalInfo}') 
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
    runFile = globals()["util"][file]["runFileName"]
    runQueryName = globals()["util"][file]["runQueryName"]
    runInputLocation = globals()["util"][file]["runInputLocation"]
    runOutputTable = globals()["util"][file]["resultSuccessTable"]
    runLocation = globals()["util"][file]["runLocation"]
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
def fCorrectDates(dataFrame, dateColumns_dict, list_dfAttention):
    '''
    gets a normalized data frame and a list of columns in a dictionary to change column type on the dataFrame
    returns a list_dfAttention a list with datetime errors, dataframe with the altered columns 
    '''
    for column in dataFrame:
        for key, value in dateColumns_dict.items():
            if column == key:
                #copy the df to errDataTime
                errDatetime = dataFrame
                #remove empty column cells
                errDatetime = errDatetime[errDatetime[column].astype(bool)]
                #reindex the errDateTime to match with mask
                errDatetime.reset_index(drop=True, inplace=True)
                
                #create a mask where the convertion to datetime fails
                mask = pd.to_datetime(errDatetime[column], format=value, errors='coerce').isna()
                
                #apply to df the mask from the substitution
                errDatetime = errDatetime[mask]

                #reindex the errDatetime
                errDatetime.reset_index(drop=True, inplace=True)

                #append dataframe to be concatenated after only if there is > 1 row in the df
                if len(errDatetime) > 0:
                    list_dfAttention.append(errDatetime)

                #the main Dataframe is kept with all the data (and the errors are coerced)
                dataFrame[column] = pd.to_datetime(dataFrame[column], format=value, errors="coerce")
    
    return dataFrame, list_dfAttention

# %%
def main(file):
    #open auth file, auth for wms is already in utilities file
    auth = open('auth.json')
    auth_load = json.load(auth)
    
    #create AzureDB connection
    engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    conn_azure = engine_azure.connect()

    #get Wms content
    util = open('utilities.json')
    utilities_load = json.load(util)
    globals()['util'] = utilities_load

    
    #wait for good response
    good_response = False
    #repeat for 503 (server out of resources) and 504 (server request timeout) responses
    while good_response == False:
        #request the response from the API
        response = setupAPIrequest(utilities_load[file])
        print('API response: %s' %response.status_code)

        if response.status_code != 504 and response.status_code != 503:
            good_response = True

    #setup global variable for the outcome of the connection
    globals()['output'] = "Failed"

    #setup a dataframe for the attention needed rows
    df_attention = pd.DataFrame()
    list_dfAttention = []

    if response.status_code == 200:
        try:
            #start cleaning of data
            df = pd.json_normalize(response.json())
            
            df, list_dfAttention = fCorrectDates(df, utilities_load[file]['dateColumns'], list_dfAttention)

            #include the rows in a sigle dataframe then remove duplicates
            if len(list_dfAttention) > 0:
                df_attention = pd.concat(list_dfAttention,  ignore_index=True)
                df_attention = df_attention.drop_duplicates()
                #adds the dataconsulta column for historic registration of the error
                df_attention['dataconsulta'] = pd.Timestamp.today().strftime('%d-%m-%Y %H:%M:%S')

            #prepare df's to upload to AzureDB
            df_attention = df_attention.fillna("")
            df = df.fillna("")

            try:
                #tsql_chunksize = 2097 // len(df.columns)
                #tsql_chunksize = 1000 if tsql_chunksize > 1000 else tsql_chunksize
                #df.to_sql(utilities_load[file]["resultSuccessTable"], engine_azure, method='multi', chunksize=tsql_chunksize, if_exists='replace', index=False)
                #best result for this process was using only fastexecutemany = true instead of multi + chunksize or any combination of that

                #insert into AzureDB the main df
                print (f'{file} starting mainInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                globals()['mainInsertTime'] = datetime.now()
                df.to_sql(utilities_load[file]["resultSuccessTable"], engine_azure, if_exists='replace', index=False)
                globals()['mainEndTime'] = datetime.now()
                
                #mark clocks
                globals()['endTime'] = datetime.now()
                globals()['attentionInsertTime'] = datetime.now()
                globals()['attentionEndTime'] = datetime.now()

                #if something needs attention
                if len(df_attention) > 0 :
                    #insert into AzureDB the attention df
                    print (f'{file} starting attentionInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                    globals()['attentionInsertTime'] = datetime.now()
                    df_attention.to_sql(utilities_load[file]["resultAttentionTable"], engine_azure, if_exists='append', index=False)
                    globals()['attentionEndTime'] = datetime.now()
                    #for the rows with date conversion errors or other filtered/coerced rows
                    attentionHandle(file, additionalInfo= "", runRowNumber= len(df_attention), engine_azure= engine_azure)

                #for the main DataFrame
                successHandle(file, additionalInfo= "", runRowNumber= len(df), engine_azure= engine_azure)
                globals()['output'] = "Success"
            except:
                globals()['endTime'] = datetime.now()
                errorSuggestedProcedureInsertAzureDB = utilities_load[file]["errorSuggestedProcedureInsertAzureDB"]
                errorDescriptionInsertAzureDB= utilities_load[file]["errorDescriptionInsertAzureDB"]
                errorHandle(2, errorDescriptionInsertAzureDB, errorSuggestedProcedureInsertAzureDB, file, engine_azure)
        except:
            globals()['endTime'] = datetime.now()
            errorSuggestedProcedureGeneric = utilities_load[file]["errorSuggestedProcedureGeneric"]
            errorDescriptionGeneric= utilities_load[file]["errorDescriptionGeneric"]
            errorHandle(2, errorDescriptionGeneric, errorSuggestedProcedureGeneric, file, engine_azure)
    else:
        globals()['endTime'] = datetime.now()
        errorSuggestedProcedureGeneric = utilities_load[file]["errorSuggestedProcedureGeneric"]
        errorDescriptionGeneric = utilities_load[file]["errorDescriptionGeneric"]
        errorHandle(1,  "External API response code: %s" %response.status_code, errorSuggestedProcedureGeneric, file, engine_azure)
    

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
    file = "wms_function_vRecebimento"
    print (f'{file} start time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    globals()['startTime'] = datetime.now()
    
    main(file)

    print('%s: done with the output: %s, runtime %s' %(file, globals()['output'], (globals()['endTime'] - globals()['startTime']).total_seconds()))


