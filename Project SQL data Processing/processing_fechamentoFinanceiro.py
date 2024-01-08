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
def executeSQL(conn_azure, sql_text):
    '''
    gets an AzureDB connection and a SQL code to run on the engine
    Returns the result query
    '''
    query_answer = conn_azure.execute(sql_text)
     
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
    runOutputTable = globals()["util"][file]["resultSuccessTable"][file]
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
            dataFrame[column].fillna("", inplace=True)
    return dataFrame, list_dfAttention

# %%
class threadMain_datapipeline_fRelFechamento(object):
    def __init__(self, numeroFilial, nomeFilial, startDate, endDate, file, engine_azure):
        #set variables
        self.numeroFilial = numeroFilial
        self.nomeFilial = nomeFilial
        self.startDate = startDate
        self.endDate = endDate
        self.engine_azure = engine_azure
        self.file = file
        #start the thread
        self.t = Thread(target=self.threadMainTask, args=())
        self.t.start()

    def getThread(self):
        return (self.t)
    
    def threadMainTask(self):
        print(f'datapipeline_fRelFechamento starting filial: {self.nomeFilial} @{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
        #wait for good response from API endpoint
        good_response = False
        #repeat for 503 (server out of resources) and 504 (server request timeout) responses
        while good_response == False:
            #make dict with extraParams
            extraParams = {"filial" : self.numeroFilial, "data_inicial" : self.startDate, "data_final" : self.endDate}

            #request the response from the API
            response = setupAPIrequest(globals()['util'][self.file], extraParams)
            print('API response: %s' %response.status_code)

            if response.status_code != 504 and response.status_code != 503:
                good_response = True
                        
        if response.status_code == 200:
            #add to the response pile
            result_list = response.json()["value"]
            adjusted_list = [dict(item, numFilial = self.numeroFilial) for item in result_list]
            globals()['response_list'] += adjusted_list
            
        else:
            errorHandle(1, "failedDataFrame", "External API response code: %s" %response.status_code, self.file, self.engine_azure)

# %%
def main_datapipeline_fRelFechamento(file, engine_azure, conn_azure, endDate, startDate):
    #open auth file for azureDB
    #auth = open('auth.json')
    #auth_load = json.load(auth)
    
    #create AzureDB connection
    #engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    #conn_azure = engine_azure.connect()

    #get utilities content
    #util = open('utilities.json')
    #utilities_load = json.load(util)
    #globals()['util'] = utilities_load

    #first and last dates of the month
    endDate = endDate.strftime("%Y-%m-%d")
    startDate = startDate.strftime("%Y-%m-%d")

    try:
        #for millennium:
        #get token
        tokenResponse = setupAPIrequest(globals()['util']["mill_getToken"], "")
        token = json.loads(tokenResponse.text)["session"]
        
        if tokenResponse.status_code != 504 and tokenResponse.status_code != 503:
            print('token load response: %s' %tokenResponse.status_code)

        #place token in the other header
        globals()['util'][file]["HTTP"]["headers"]["wts-session"] = token
    except:
        print("token is broken")
        
    #get list of filiais from AzureDB
    globals()['response_list'] = []
    threads = []
    list_filiais = executeSQL(conn_azure, sql_list["get_filiaisList_datapipeline_fRelFechamento"])
    for row in list_filiais.all():
        n_coluna = 0
        for coluna in list_filiais.keys():
            if coluna == "numFilial":
                numeroFilial = row[n_coluna]
            if coluna == "nomeProcesso":
                nomeFilial = row[n_coluna]
            n_coluna = n_coluna + 1
        t = threadMain_datapipeline_fRelFechamento(numeroFilial, nomeFilial, startDate, endDate, file, engine_azure)
        threads.append(t.getThread())
    
    for t in threads:
        t.join()

    #setup global variable for the outcome of the connection
    globals()['output'] = "Failed"    
    
    list_dfAttention = []      
    
    #the dataframe could be used instead of uploading to azure then querying in azure
    try:
        #start cleaning/changing dType of data
        df = pd.json_normalize(globals()['response_list'])

        df, list_dfAttention = fCorrectTypes(df, globals()['util'][file]["columnsType_dict"], list_dfAttention)

        if len(globals()['response_list']) > 0:
            try:
                #insert into AzureDB the main df
                print (f'{file} starting datapipeline_fRelFechamento mainInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                df.to_sql(globals()['util'][file]["resultSuccessTable"][file], engine_azure, if_exists='replace', index=False)
                
                globals()['output'] = "Success"
            except:
                errorHandle(2, "insertAzureDB_datapipeline_fRelFechamento", None, file, engine_azure)
        else:
            globals()['output'] = 'Success'
    except:
        errorHandle(2, "failedDataFrame_datapipeline_fRelFechamento", None, file, engine_azure)
    
    print ('finished datapipeline_fRelFechamento')

# %%
class threadMain(object):
    def __init__(self, numFilial, nomeProcesso, nomeFilialMill, codFilialMill, startDate, endDate, file, engine_azure):
        #set variables
        self.numFilial = numFilial
        self.nomeFilialMill = nomeFilialMill
        self.codFilialMill = codFilialMill
        self.nomeProcesso = nomeProcesso

        #sets variables for the rules
        self.list_regrasGMV = []
        self.list_regrasOther = []

        #set dates
        self.startDateDay = startDate.day
        self.startDateMonth = startDate.month
        self.startDateYear = startDate.year
        self.endDateDay = endDate.day
        self.endDateMonth = endDate.month
        self.endDateYear = endDate.year
        
        #set engine and connection
        self.engine_azure = engine_azure
        self.conn_azure = self.engine_azure.connect()

        self.file = file
        #start the thread
        self.t = Thread(target=self.threadMainTask, args=())
        self.t.start()

    def getThread(self):
        return (self.t)
    
    def getAllRules(self):
        #getGMV
        list_getRegrasGMV = executeSQL(self.conn_azure, sql_list["getGMV"] %self.nomeProcesso)
        for row in list_getRegrasGMV:
            nCol = 0
            for column in list_getRegrasGMV.keys():
                if column == 'mainidentifier':
                    rule = row[nCol]
                if column == 'subidentifier':
                    subrule = row[nCol]
                nCol += 1
            self.list_regrasGMV += [{'rule' : rule ,'subrule' : subrule}]
        self.list_regrasGMV = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in self.list_regrasGMV)]

        list_getRegrasOther = executeSQL(self.conn_azure, sql_list["getOthers"] %self.nomeProcesso)
        for row in list_getRegrasOther:
            nCol = 0
            for column in list_getRegrasOther.keys():
                if column == 'mainrule':
                    rule = row[nCol]
                if column == 'subrule':
                    subrule = row[nCol]
                nCol += 1
            self.list_regrasOther += [{'rule' : rule, 'subrule' : subrule}]
        self.list_regrasOther = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in self.list_regrasOther)]

        
    def threadMainTask(self):
        if self.nomeProcesso != None and self.nomeProcesso != "":
            print(f'starting filial: {self.numFilial} @{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
            self.getAllRules()
        
        #do GMV rules
        for mainRule in self.list_regrasGMV:
            rule = mainRule['rule']
            subrule = mainRule['subrule']
            found = False
            match rule:
                case "SR":
                    match subrule:
                        case "MP":
                            list_answer = executeSQL(self.conn_azure, sql_list["GMV_SR_MP"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "TM":
                    match subrule:
                        case "MP":
                            list_answer = executeSQL(self.conn_azure, sql_list["GMV_TM_MP"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "SP":
                            list_answer = executeSQL(self.conn_azure, sql_list["GMV_TM_SP"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "MF":
                            list_answer = executeSQL(self.conn_azure, sql_list["GMV_TM_MF"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "VNF":
                    match subrule:
                        case "SP":  
                            list_answer = executeSQL(self.conn_azure, sql_list["GMV_VNF_SP"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
            if found:
                for row in list_answer:
                    globals()['response_list'] += [row]
        #do otherRules
        for otherRule in self.list_regrasOther:
            rule = otherRule['rule']
            subrule = otherRule['subrule']
            found = False
            match rule:
                case "ARMZ":
                    match subrule:
                        case "LC_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["ARMZ_LCPDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "SG_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["ARMZ_SGPDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "MOV":
                    match subrule:
                        case "MOV_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["MOV_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "MOV_SC":
                            list_answer = executeSQL(self.conn_azure, sql_list["MOV_SC"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "FRETE":
                    match subrule:
                        case "FRT_CTE":
                            list_answer = executeSQL(self.conn_azure, sql_list["FRT_CTE"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "FRT_COR":
                            list_answer = executeSQL(self.conn_azure, sql_list["FRT_COR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "VTEX":
                    match subrule:
                        case "VTX_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["VTX_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "AA_FIXO":
                            list_answer = executeSQL(self.conn_azure, sql_list["AA_FIXO"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "WTL_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["WTL_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "SAC":
                    match subrule:
                        case "SAC_FX":
                            list_answer = executeSQL(self.conn_azure, sql_list["SAC_FX"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "SAC_TVGMV":
                            list_answer = executeSQL(self.conn_azure, sql_list["SAC_TVGMV"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                        case "SAC_HRX":
                            list_answer = executeSQL(self.conn_azure, sql_list["SAC_HRX"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "ESTORNO":
                    match subrule:
                        case "EST_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["EST_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "CADPROD":
                    match subrule:
                        case "CAD_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["CAD_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "SAB":
                    match subrule:
                        case "SAB_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["SAB_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "INV":
                    match subrule:
                        case "INV_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["INV_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
                case "REEMBOLSO":
                    match subrule:
                        case "RMB_PDR":
                            list_answer = executeSQL(self.conn_azure, sql_list["RMB_PDR"].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
                            found = True
            if found:
                for row in list_answer:
                    globals()['response_list'] += [row]
        #
        #do relatorio analitico
        list_analytic_reports = ["GMV_REL", "ARMZ_REL", "MOV_REL", "ESTDIA_REL", "FRTCTE_REL", "FRTCOR_REL"]
        for item in list_analytic_reports:
            list_answer = executeSQL(self.conn_azure, sql_list[item].format(startyear = self.startDateYear, startmonth = self.startDateMonth, startday = self.startDateDay, endyear = self.endDateYear, endmonth = self.endDateMonth, endday = self.endDateDay,  numerofilial = self.numFilial))
            
            for row in list_answer:
                globals()['response_list_analytic_report'] += [row]
        

# %%
def sqlcol(dfparam):    
    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sa.types.VARCHAR(length=5000)})
                                 
        if "datetime" in str(j):
            dtypedict.update({i: sa.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sa.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sa.types.INT()})

    return dtypedict

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

    #first and last dates of the month
    this_date = datetime(2023, 11, 1)
    that_date = datetime(2023, 12, 1)
    #endDate = (datetime.today().replace(day=1) - timedelta(days=1))
    #startDate = ((datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1))

    #endDate = (this_date.replace(day=1) - timedelta(days=1))
    #startDate = ((this_date.replace(day=1) - timedelta(days=1)).replace(day=1))

    endDate = that_date
    startDate = this_date

    print ('enddate : ', endDate)
    print ('startDate : ', startDate)

    #makes the pipeline call
    main_datapipeline_fRelFechamento('mill_fRelFechamento', engine_azure, conn_azure, endDate, startDate)

    #get list of filiais of fechamento from AzureDB 
    globals()['response_list'] = []
    globals()['response_list_analytic_report'] = []
    threads = []
    list_filiais = executeSQL(conn_azure, sql_list["getFiliaisList"])
    for row in list_filiais.all():
        n_coluna = 0
        for coluna in list_filiais.keys():
            if coluna == "numFilial":
                numFilial = row[n_coluna]
            if coluna == "nomeFilialMill":
                nomeFilialMill = row[n_coluna]
            if coluna == "codFilialMill":
                codFilialMill = row[n_coluna]
            if coluna == "nomeProcesso":
                nomeProcesso = row[n_coluna]
            n_coluna = n_coluna + 1
        t = threadMain(numFilial, nomeProcesso, nomeFilialMill, codFilialMill, startDate, endDate, file, engine_azure)
        threads.append(t.getThread())
    
    for t in threads:
        t.join()

    #setup global variable for the outcome of the connection
    globals()['output'] = "Failed"
    
    #insert to azureDB
    list_dfAttention = []      
    try:
        #start cleaning/changing dType of data
        df_main = pd.DataFrame(globals()['response_list'])
        df_analytic_report = pd.DataFrame(globals()['response_list_analytic_report'])
        
        df_main, list_dfAttention = fCorrectTypes(df_main, globals()['util'][file]["columnsType_dict"]["processing_FechamentoFinanceiro"], list_dfAttention)
        df_analytic_report, list_dfAttention = fCorrectTypes(df_analytic_report, globals()['util'][file]["columnsType_dict"]["processing_FechamentoAnalyticReport"], list_dfAttention)
        
        df_main_dtypes = sqlcol(df_main)
        df_analytic_report_dtypes = sqlcol(df_analytic_report)

        if len(globals()['response_list']) > 0:
            try:
                #insert into AzureDB the main df
                print (f'{file} starting mainInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                globals()['mainInsertTime'] = datetime.now()

                #main stuff
                df_main.to_sql(utilities_load[file]["resultSuccessTable"]["processing_FechamentoFinanceiro"], engine_azure, if_exists='replace', index=False, dtype = df_main_dtypes)
                globals()['mainEndTime'] = datetime.now()

                #analytic report stuff
                df_analytic_report.to_sql(utilities_load[file]["resultSuccessTable"]["processing_FechamentoAnalyticReport"], engine_azure, if_exists='replace', index=False)
                
                #mark clocks
                globals()['endTime'] = datetime.now()
                globals()['attentionInsertTime'] = datetime.now()
                globals()['attentionEndTime'] = datetime.now()

                #for the main DataFrame
                #successHandle(file= file, additionalInfo= "", runRowNumber= len(df), engine_azure= engine_azure)
                globals()['output'] = "Success"
            except:
                errorHandle(2, "insertAzureDB", None, file, engine_azure)
        else:
            #if there is no data after TransID then call successHandle
            globals()["max_identifiervalue"] = None
            globals()['mainInsertTime'] = datetime.now()
            globals()['mainEndTime'] = datetime.now()
            globals()['output'] = 'Success'
            #successHandle(file= file, additionalInfo= "no new trans_id", runRowNumber= len(df), engine_azure = engine_azure)
    except:
        print('nayyyyyy')
        #errorHandle(2, "failedDataFrame", None, file, engine_azure)
    

# %%
if __name__ == "__main__":
    file = "processing_FechamentoFinanceiro"
    print (f'{file} start time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    globals()['startTime'] = datetime.now()
    
    main(file)
    globals()['endTime'] = datetime.now()
    print('%s: done with the output: %s, runtime %s' %(file, globals()['output'], (globals()['endTime'] - globals()['startTime']).total_seconds()))


