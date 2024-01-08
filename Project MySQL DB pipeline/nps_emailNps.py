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
                    engine = create_engine(quoted, fast_executemany=True).execution_options(isolation_level="AUTOCOMMIT")

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
def executeSQL(conn, sql_text):
    '''
    gets an AzureDB connection and a SQL code to run on the engine
    Returns the result query
    '''

    query_answer = conn.execute(sql_text)
     
    return query_answer


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
                if data_type == "to_numeric":
                    #remove commas in case the numbers are stored as string
                    dataFrame[column] = dataFrame[column].replace(regex = {'[A-Za-z/.]', ''}).replace(regex = {',', '.'})
                    #change dType
                    dataFrame[column] = pd.to_numeric(dataFrame[column], errors='coerce')
                break
        if dataFrame[column].dtype == int or dataFrame[column].dtype == float :
            dataFrame[column].fillna(0, inplace=True)
        else:
            dataFrame[column].fillna("", inplace=True)
                
    
    return dataFrame, list_dfAttention

# %%
class threadMain:
    def __init__(self, functionName, queryName, identifier, identifierValue, file, engine_query, engine_azure):
        #set variables
        self.identifier = identifier
        self.identifierValue = identifierValue
        self.functionName = functionName
        self.queryName = queryName
        self.engine_query = engine_query
        self.engine_azure = engine_azure
        self.file = file
        #start the thread
        self.t = Thread(target=self.threadMainTask, args=())
        self.t.start()

    def getThread(self):
        return (self.t)
    
    def threadMainTask(self):
        print(f'starting {self.functionName} @{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
        
        #get data from npsDB
        output = executeSQL(self.engine_query, sql_list[self.queryName] %self.identifierValue)
        
        #start a dataframe to insert in temp_tables

        list_dfAttention = []
        
        try:
            df = pd.DataFrame(output)
            if len(df) > 0:
                #start data-formatting
                print ('starting dataType formatting')
                df, list_dfAttention = fCorrectTypes(df, globals()['util'][self.file]["columnsType_dict"][self.functionName], list_dfAttention)

                #include the rows in a sigle dataframe then remove duplicates
                if len(list_dfAttention) > 0:
                    df_attention = pd.concat(list_dfAttention, ignore_index=True)
                    df_attention = df_attention.drop_duplicates()
                    #adds the dataconsulta column for historic registration of the error
                    df_attention['dataconsulta'] = pd.Timestamp.today().strftime('%d-%m-%Y %H:%M:%S')
                    df_attention = df_attention.fillna("")

                #sortby to get last trans_id value
                df = df.sort_values(by=['ID_NPS'], ascending=True)

                try:
                    #check for identifier
                    if globals()['util'][self.file]["nfo"]["hasIdentifier"] == "y":
                        globals()["max_identifiervalue"] = df[self.identifier].max()
                    
                    #insert into AzureDB the main df
                    print (f'{self.functionName} starting mainInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                    
                    globals()['mainInsertTime'] = datetime.now()
                    df = df.drop(columns=[self.identifier])
                    print (globals()['util'][self.file]["resultTempSuccessTable"][self.functionName])
                    
                    df.to_sql(globals()['util'][self.file]["resultTempSuccessTable"][self.functionName], self.engine_azure, if_exists='replace', index=False)
                    
                    globals()['mainEndTime'] = datetime.now()
                    globals()['rownumber'] = len(df)

                    #for the main DataFrame - only call successHandle after the insertion is a success
                    #successHandle(file= self.file, additionalInfo= "", runRowNumber= len(df), engine_azure = self.engine_azure)
                    globals()['output'] += [{self.functionName : "Success"}]
                except:
                    globals()['output'] += [{self.functionName : "failed_insertAzureDB"}]
                    #errorHandle(2, "insertAzureDB", None, self.file, self.engine_azure)
            else:
                #if there is no data after TransID then call successHandle
                globals()['output'] += [{self.functionName : 'allClear'}]
        except:
            globals()['output'] += [{self.functionName : "failed_DataFrame"}]
            #errorHandle(2, "failedDataFrame", None, self.file, self.engine_azure)
        
        
        

# %%
def main(file):
    #open auth file for azureDB
    auth = open('auth.json')
    auth_load = json.load(auth)
    
    #create AzureDB connection
    engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    conn_azure = engine_azure.connect()

    #create npsDB connection
    engine_nps = getConnforMYSQL(auth_load, "npsAccess")
    conn_nps = engine_nps.connect()

    #get utilities content
    util = open('utilities.json')
    utilities_load = json.load(util)
    globals()['util'] = utilities_load

    list_dfAttention = []

    #get identifier data
    try:
        if globals()['util'][file]["nfo"]["hasIdentifier"] == "y":
            print ("identifier detected")
            sql_text = sql_list["get_identifierData"] %globals()['util'][file]['nfo']['runQueryName']
            identifierData = executeSQL(conn_azure, sql_text)

            #this should never return more than one line (because it gets num_linha = 1)
            for row in identifierData.all():
                identifier = row._mapping["identifier"]
                identifierValue = row._mapping["identifierValue"]

            if identifier == "" or identifierValue == "" or (identifier != globals()['util'][file]["nfo"]["identifier"] and identifier != globals()['util'][file]["nfo"]["identifier"].Uppercase()):
                raise Exception
        else:
            print ("identifier Not detected")
    except:
        print ( ":)" )

    list_queries = globals()['util'][file]['queries']
    threads = []
    #setup output list for tracking
    globals()['output'] = []
    
    for key, value in list_queries.items():
        t = threadMain(key, value, identifier, identifierValue, file, engine_nps, engine_azure)
        thread = t.getThread()
        threads.append(thread)
        thread.join()
    
    #after they finish, start merging (as upsert) temporary tables to the tables
    output = None
    for items in globals()['output']:
        for key, value in items.items():
            if value not in ['Success', 'allClear']:
                output = 'failed'
            else:
                if value == 'Success' and output != 'failed':
                    output = 'Success'
                elif value == 'allClear' and output != 'failed':
                    output = 'allClear'

    #if all outputs are success then
    if output == 'Success':
        #do data merging with the temp tables
        globals()['mainInsertTime'] = datetime.now()
        #merge emails npsData
        answer = executeSQL(conn_azure, sql_list['merge_emailsNpsData'])

        #merge respostas npsData
        answer = executeSQL(conn_azure, sql_list['merge_repostasNpsData'])
        globals()['mainEndTime'] = datetime.now()

        successHandle(file = file, additionalInfo= "", runRowNumber = globals()['rownumber'], engine_azure = engine_azure)
        globals()['output'] = 'Success'


    elif output == 'allClear':
        #dont do data merging with the temp tables
        #if there is no data after TransID then call successHandle
        globals()["max_identifiervalue"] = identifierValue
        globals()['mainInsertTime'] = datetime.now()
        globals()['mainEndTime'] = datetime.now()
        successHandle(file = file, additionalInfo= "no new id_nps", runRowNumber= 0, engine_azure = engine_azure)
        globals()['output'] = 'Success'

    elif output == 'failed':
        globals()['output'] = 'Failed'
        
    print ("yey: " , globals()['output'])
            

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
    file = "nps_emailNps"
    print (f'{file} start time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    globals()['startTime'] = datetime.now()
    globals()['output'] = "Failed"
    
    main(file)

    globals()['endTime'] = datetime.now()
    print('%s: done with the output: %s, runtime %s' %(file, globals()['output'], (globals()['endTime'] - globals()['startTime']).total_seconds()))


