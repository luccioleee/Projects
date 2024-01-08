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
import math

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
#get response from API- 
def setupAPIrequest(utilities, extraParams, overrideHeaders, ending):
    '''
    utilities: the utilies file
    extraParams: extraParams as Dictionary for adding params in the request
    overrideHeaders: overrideHeaders as Dictionary for changing headers
    ending: as string
    '''
    schemeHTTP = utilities["HTTP"]["schemeHTTP"]
    baseHTTP = utilities["HTTP"]["baseHTTP"]
    extraHTTP = utilities["HTTP"]["extraHTTP"]
    headers = utilities["HTTP"]["headers"]
    
    #overrideHeaders
    if overrideHeaders != "":
        for key, value in overrideHeaders.items():
            for header_key, header_value in headers.items():
                if header_key == key:
                    headers[key] = value

    #check if there is params variables:
    paramsHTTP = ""
    for key, value in utilities["HTTP"].items():
        if key == "params":
            for key, value in utilities["HTTP"]["params"].items():
                paramsHTTP = paramsHTTP + key + "=" + str(value) + "&"
            paramsHTTP = "?" + paramsHTTP
    if extraParams != "":
        paramsHTTP = paramsHTTP + "?"
        for key, value in extraParams.items():
            paramsHTTP = paramsHTTP + key + "=" + str(value) + "&"
        paramsHTTP = paramsHTTP[:-1]
    completeHTTP = schemeHTTP + baseHTTP + extraHTTP + paramsHTTP + ending

    print (completeHTTP)
    
    if utilities["HTTP"]["method"] == "get":
        response = requests.get(completeHTTP, headers=headers)
    if utilities["HTTP"]["method"] == "post":
        response = requests.post(completeHTTP, headers=headers)
    
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
            dataFrame[column].fillna("", inplace=True)
    return dataFrame, list_dfAttention

# %%
class subthreadMain:
    def __init__(self, client , file, engine_azure, extraparams, overrideHeaders, ending, extra_nfo, mode):
        self.client = client

        self.file = file
        self.response = None
        self.response_dict = None

        self.extraparams = extraparams
        self.overrideHeaders = overrideHeaders
        self.ending = ending

        #set engine and connection
        self.engine_azure = engine_azure
        self.conn_azure = self.engine_azure.connect()

        if mode == "order_task" :
            self.t = Thread(target=self.subthreadOrderTask, args=())
            self.t.start()
        elif mode == "shipment_task":
            self.this_shipment = extra_nfo
            if self.ending != 'None':
                self.t = Thread(target=self.subthreadShipmentTask, args=())
                self.t.start()
            else:
                self.t = Thread(target=self.subthreadskip, args=())
                self.t.start()
        elif mode == "reputation_task":
            self.t = Thread(target=self.subthreadReputationTask, args=())
            self.t.start()

    def getsubThread(self):
        return (self.t)
    
    def subthreadOrderTask(self):
        #print (f"starting page: {self.extraparams['offset']}")
        self.response = setupAPIrequest(globals()['util']['meli_get_seller_orders'], self.extraparams, self.overrideHeaders, self.ending)
        self.response_dict = json.loads(self.response.text)
        print('next block:')
        #print (self.response_dict['results'])
        #for item in self.response_dict['results']:
        #    print(item['id'])
        #create the orders list of client
        for each_order in self.response_dict['results']:
            #get order id
            new_order = order()
            new_order.rawtext = each_order
            new_order.order_id = each_order['id']
            new_order.status = each_order['status']
            #get payment nfo
            for each_payment in each_order['payments']:
                new_payment = payment()
                new_payment.payment_approvation_date = each_payment['date_approved']
                new_payment.payment_status = each_payment['status']
                new_payment.payment_id = each_payment['id']
                new_payment.rawtext = each_payment
                new_order.list_payments += [new_payment]
            #get items nfo
            for each_item in each_order['order_items']:
                new_item = item()
                new_item.rawtext = each_item
                new_order.list_items += [new_item]
            #get shipment nfo
            new_shipment = shipment()
            new_shipment.shipment_id = each_order['shipping']['id']
            new_order.list_shipments += [new_shipment]

            #add to orders list
            self.client.orders_list += [new_order]

            print(new_order.order_id)
    
    def subthreadskip(self):
        pass

    def subthreadShipmentTask(self):
        response = setupAPIrequest(globals()['util']['meli_get_shipping_nfo'], self.extraparams, self.overrideHeaders, self.ending)
        response_dict = json.loads(response.text)
        self.this_shipment.rawtext = response_dict
        self.this_shipment.estimated_delivery_limit = response_dict['shipping_option']['estimated_delivery_limit']
        self.this_shipment.estimated_handling_limit = response_dict['shipping_option']['estimated_handling_limit']
        self.this_shipment.shipment_id = response_dict['id']
        self.this_shipment.shipment_substatus = response_dict['substatus']
        self.this_shipment.shipment_status = response_dict['status']
        self.this_shipment.tracking_number = response_dict['tracking_number']
        self.this_shipment.tracking_method = response_dict['tracking_method']

    def subthreadReputationTask(self):
        response = setupAPIrequest(globals()['util']['meli_get_seller_reputation'], self.extraparams, self.overrideHeaders, self.ending)
        response_dict = json.loads(response.text)
        #only one reputation for each seller
        this_seller_reputation = reputation()
        this_seller_reputation.rawtext = response_dict
        this_seller_reputation.rawtext['result_date'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        this_seller_reputation.level_id = response_dict['seller_reputation']['level_id']
        this_seller_reputation.result_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        #the reputation_raw needs a column of date to track evolution of the reputation

        self.client.seller_reputation = this_seller_reputation

# %%
class threadMain:
    def __init__(self, client , file, engine_azure):
        #set variables
        self.client = client

        #sets variables for the rules
        self.list_regrasGMV = []
        self.list_regrasOther = []

        #set dates
        
        #set engine and connection
        self.engine_azure = engine_azure
        self.conn_azure = self.engine_azure.connect()

        #initialize client subthreads
        self.subthreads = []

        self.file = file
        #start the thread
        self.t = Thread(target=self.threadMainTask, args=())
        self.t.start()

    def getThread(self):
        return (self.t)

    def _test_access_token(self):
        overrideHeaders = ""
        ending = ""
        extraparams = {
            'grant_type' : 'refresh_token',
            'client_id' : self.client._client_id,
            'client_secret' : self.client._client_secret,
            'refresh_token' : self.client.new_refresh_token
            }
        response = setupAPIrequest(globals()['util']['meli_get_access_token'], extraparams, overrideHeaders, ending)
        if response.status_code == 400:
            response_dict = json.loads(response.text)
            if response_dict['error'] == 'invalid_grant':
                #try with the refresh_token_payload
                self.client.new_refresh_token = self.client._refresh_token_payload
                overrideHeaders = ""
                ending = ""
                extraparams = {
                    'grant_type' : 'refresh_token',
                    'client_id' : self.client._client_id,
                    'client_secret' : self.client._client_secret,
                    'refresh_token' : self.client.new_refresh_token
                    }
                response = setupAPIrequest(globals()['util']['meli_get_access_token'], extraparams, overrideHeaders, ending)
                if response.status_code != 200:
                    print ('token inválido')


    def _getaccesstoken(self):
        self._test_access_token()
        #get access token from meli
        overrideHeaders = ""
        ending = ""
        extraparams = {
            'grant_type' : 'refresh_token',
            'client_id' : self.client._client_id,
            'client_secret' : self.client._client_secret,
            'refresh_token' : self.client.new_refresh_token
            }
        response = setupAPIrequest(globals()['util']['meli_get_access_token'], extraparams, overrideHeaders, ending)
        #response is in json format
        response_dict = json.loads(response.text)
        #update client class with access token and new info
        self.client.access_token = response_dict['access_token']
        self.client.used_refresh_token = self.client.new_refresh_token
        self.client.new_refresh_token = response_dict['refresh_token']
        self.client.user_id = response_dict['user_id']

    def _getordersfromseller(self): #todo -> dividir por thread por página para acelerar os downloads, descobrir porque ele fica vindo pedido repetido embora muda de página
        #seller as same as client
        #get orders from meli
        time_from = (datetime.now() - timedelta(days = 7)).strftime("%Y-%m-%d") + "T00:00:00-00:00"
        time_to = datetime.now().strftime("%Y-%m-%d") + "T" + datetime.now().strftime("%H:%M:%S") + "-00:00"
        print ("time_from :", time_from)
        print ("time_to :", time_to)    
        ending = ""
        extraparams = {
            'seller' : self.client.user_id,
            'order.date_created.from' : time_from,
            'order.date_created.to'   : time_to,
            }
        #add a header
        overrideHeaders = {
            "Authorization": "Bearer %s" %self.client.access_token
            }
        response = setupAPIrequest(globals()['util']['meli_get_seller_orders'], extraparams, overrideHeaders, ending)
        response_dict = json.loads(response.text)
        
        #page_number
        print (f"expected total orders: {response_dict['paging']['total']}")
        retrieve_limit = 50
        page_number = response_dict["paging"]["total"]/retrieve_limit
        for page in range(math.ceil(page_number)):
            extraparams['offset'] = page*retrieve_limit
            extraparams['limit'] = retrieve_limit
            extraparams['sort'] = 'date_desc'

            t = subthreadMain(self.client, self.file, self.engine_azure, extraparams, overrideHeaders, ending, None, "order_task")
            self.subthreads.append(t.getsubThread())
            
            for t in self.subthreads:
                t.join()

    def _getshippingnfo(self):         
        #get shipping information of the order
        for each_order in self.client.orders_list:
            for each_shipment in each_order.list_shipments:

                shipment_id = each_shipment.shipment_id
                this_shipment = each_shipment

                extraparams = {
                    }
                #add a header
                overrideHeaders = {
                    "Authorization": "Bearer %s" %self.client.access_token
                    }
                ending = str(shipment_id)

                t = subthreadMain(self.client, self.file, self.engine_azure, extraparams, overrideHeaders, ending, this_shipment, "shipment_task")
                self.subthreads.append(t.getsubThread())

                for t in self.subthreads:
                    t.join()
                
    def _getsellerreputation(self):
        extraparams = {
            }
        #add a header
        overrideHeaders = {
            "Authorization": "Bearer %s" %self.client.access_token
            }
        ending = str(self.client.user_id)

        t = subthreadMain(self.client, self.file, self.engine_azure, extraparams, overrideHeaders, ending, None, "reputation_task")
        self.subthreads.append(t.getsubThread())

        for t in self.subthreads:
            t.join()
        
    def _remove_keys(self, dictionary, keys_to_remove):
        #to remove keys of a dictionary
        return {k: v for k, v in dictionary.items() if k not in keys_to_remove}
    
    def threadMainTask(self):
        #get the access token from meli
        self._getaccesstoken()

        #insert into newTokens list for insertion on nfo_meliTokenTable (to keep updated)
        globals()['list_response_newTokens'] += [{
            'client_id': self.client._client_id,
            'used_refresh_token': self.client.used_refresh_token,
            'refresh_time': datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
            'new_refresh_token': self.client.new_refresh_token
            }]

        #get orders from meli of the client
        self._getordersfromseller()

        #get reputation of the client
        self._getsellerreputation()

        #get shipping nfo of the client
        self._getshippingnfo()

        #start data processing (T)
        #this is specific for asked table
        result_client_name = self.client._client_name
        result_client_id = self.client.client_id
        for each_order in self.client.orders_list:
            result_order_id = each_order.order_id
            for each_shipment in each_order.list_shipments:
                globals()['list_summaryTable_shipments'] += [{
                    'result_client_name' : result_client_name,
                    'result_client_id' : result_client_id,
                    'result_order_id' : result_order_id,
                    'result_shipment_id' : each_shipment.shipment_id,
                    'result_estimated_delivery_limit' : each_shipment.estimated_delivery_limit,
                    'result_estimated_handling_limit' : each_shipment.estimated_handling_limit,
                    'result_shipment_status' : each_shipment.shipment_status,
                    'result_shipment_shipment_substatus' : each_shipment.shipment_substatus,
                    'result_shipment_tracking_number' : each_shipment.tracking_number,
                    'result_shipment_tracking_method' : each_shipment.tracking_method
                }]
                #insert shipments info into raw table
                globals()['list_rawTable_shipments'] += [each_shipment.rawtext]

            
            for each_payment in each_order.list_payments:
                globals()['list_summaryTable_payments'] += [{
                    'result_client_name' : result_client_name,
                    'result_client_id' : result_client_id,
                    'result_order_id' : result_order_id,
                    'result_payment_id' : each_payment.payment_id,
                    'result_payment_status' : each_payment.payment_status,
                    'result_payment_approvation_date' : each_payment.payment_approvation_date
                }]
                
            #this is to create the generic table (with all nfo)
            #print (each_order)
            globals()['list_rawTable_payments'] += each_order.rawtext['payments']
            
            #remove certain keys of each_order to make the rawTable_orders
            invalid = {"payments", "order_items","tags"}
            result_order = self._remove_keys(each_order.rawtext, invalid)
            globals()['list_rawTable_orders'] += [result_order]
        
        #place seller reputation on the global table
        #for summarized table
        globals()['list_summaryTable_reputation'] += [{
            'result_client_id' : result_client_id,
            'result_client_name' : result_client_name,
            'result_date' : self.client.seller_reputation.result_date,
            'result_level_id' : self.client.seller_reputation.level_id
        }]
        #for generic table
        globals()['list_rawTable_reputation'] += [self.client.seller_reputation.rawtext]
            
        

# %%
class order:
    def __init__(self):
        self.order_id = None
        self.rawtext = None
        self.status = None
        self.list_shipments = []
        self.list_payments = []
        self.list_items = []

# %%
class item:
    def __init__(self):
        self.rawtext = None

# %%
class payment:
    def __init__(self):
        self.payment_status = None
        self.payment_approvation_date = None
        self.payment_id = None
        self.rawtext = None

# %%
class shipment:
    def __init__(self):
        self.rawtext = None
        self.shipment_id = None
        self.tracking_number = None
        self.estimated_delivery_limit = None
        self.estimated_handling_limit = None
        self.shipment_status = None
        self.shipment_substatus = None
        self.tracking_method = None

# %%
class client:
    def __init__(self, refresh_id, client_id, used_refresh_token, new_refresh_token) -> None:
        #set variables:
        self._client_id = client_id
        self.refresh_id = refresh_id
        self.used_refresh_token = used_refresh_token
        self.new_refresh_token = new_refresh_token
        self._client_name = None
        self._client_secret = None
        self._refresh_token_payload = None
        self.access_token = None
        self.user_id = None
        self.orders_list = []
        self.seller_reputation = None

    @property
    def client_id(self):
        return self._client_id

    def add_other_variables(self, client_name, client_secret, refresh_token_payload):
        self._client_name = client_name
        self._client_secret = client_secret
        self._refresh_token_payload = refresh_token_payload

# %%
class reputation:
    def __init__(self):
        self.rawtext = None
        self.result_date = None
        self.level_id = None

# %%
def df_upsert(data_frame, table_name, engine, schema=None, match_columns=None):
    """
    Perform an "upsert" on a SQL Server table from a DataFrame.
    Constructs a T-SQL MERGE statement, uploads the DataFrame to a
    temporary table, and then executes the MERGE.
    Parameters
    ----------
    data_frame : pandas.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    match_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    """
    table_spec = ""
    if schema:
        table_spec += "[" + schema.replace("]", "]]") + "]."
    table_spec += "[" + table_name.replace("]", "]]") + "]"

    df_columns = list(data_frame.columns)
    if not match_columns:
        insp = sa.inspect(engine)
        match_columns = insp.get_pk_constraint(table_name, schema=schema)[
            "constrained_columns"
        ]
    columns_to_update = [col for col in df_columns if col not in match_columns]
    stmt = f"MERGE {table_spec} WITH (HOLDLOCK) AS main\n"
    stmt += f"USING (SELECT {', '.join([f'[{col}]' for col in df_columns])} FROM #temp_table) AS temp\n"
    join_condition = " AND ".join(
        [f"main.[{col}] = temp.[{col}]" for col in match_columns]
    )
    stmt += f"ON ({join_condition})\n"
    stmt += "WHEN MATCHED THEN\n"
    update_list = ", ".join(
        [f"[{col}] = temp.[{col}]" for col in columns_to_update]
    )
    stmt += f"  UPDATE SET {update_list}\n"
    stmt += "WHEN NOT MATCHED THEN\n"
    insert_cols_str = ", ".join([f"[{col}]" for col in df_columns])
    insert_vals_str = ", ".join([f"temp.[{col}]" for col in df_columns])
    stmt += f"  INSERT ({insert_cols_str}) VALUES ({insert_vals_str});"

    with engine.begin() as conn:
        data_frame.to_sql("#temp_table", conn, index=False)
        conn.exec_driver_sql(stmt)
        conn.exec_driver_sql("DROP TABLE IF EXISTS #temp_table")

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
    
    #creat a list to insert new tokens in nfo_meliTokenTable
    globals()['list_response_newTokens'] = []

    globals()['list_summaryTable_payments'] = []
    globals()['list_summaryTable_shipments'] = []
    globals()['list_summaryTable_reputation'] = []
    globals()['list_rawTable_payments'] = []
    globals()['list_rawTable_shipments'] = []
    globals()['list_rawTable_reputation'] = []
    
    globals()['list_rawTable_orders'] = []

    #create a clients list
    list_clients = []

    #get list of ongoing tokens
    list_tokens = executeSQL(conn_azure, sql_list["getongointokenslist"])
    for row in list_tokens.all():
        n_coluna = 0
        for coluna in list_tokens.keys():
            if coluna == "refresh_id":
                refresh_id = row[n_coluna]
            if coluna == "client_id":
                client_id = row[n_coluna]
            if coluna == "used_refresh_token":
                used_refresh_token = row[n_coluna]
            if coluna == "new_refresh_token":
                new_refresh_token = row[n_coluna]
            n_coluna = n_coluna + 1
        new_client = client(refresh_id, client_id, used_refresh_token, new_refresh_token)
        list_clients += [new_client]

    #get list of clients from AzureDB 
    globals()['response_list'] = []
    threads = []
    list_filiais = executeSQL(conn_azure, sql_list["getclientslist"])
    for row in list_filiais.all():
        n_coluna = 0
        #first find the client in client list
        for coluna in list_filiais.keys():
            if coluna == "client_id":
                client_id = row[n_coluna]
                client_not_found = True
                for each_client in list_clients:
                    if each_client._client_id == client_id :
                        this_client = each_client
                        client_not_found = False
                        break
            n_coluna += 1
        n_coluna = 0 
        #then find every information of the client
        for coluna in list_filiais.keys():
            if coluna == "client_name":
                client_name = row[n_coluna]
            if coluna == "client_secret":
                client_secret = row[n_coluna]
            if coluna == "refresh_token_payload":
                refresh_token_payload = row[n_coluna]
            if coluna == "client_id":
                client_id = row[n_coluna]
            n_coluna = n_coluna + 1

        #update client information
        if client_not_found:
            #create a client - that was recently inserted
            refresh_id = None
            used_refresh_token = refresh_token_payload
            new_refresh_token = refresh_token_payload
            this_client = client(refresh_id, client_id, used_refresh_token, new_refresh_token)
            this_client.add_other_variables(client_name, client_secret, refresh_token_payload)
        else:
            this_client.add_other_variables(client_name, client_secret, refresh_token_payload)
        t = threadMain(this_client, file, engine_azure)
        threads.append(t.getThread())
    
    for t in threads:
        t.join()

    #setup global variable for the outcome of the connection
    globals()['output'] = "Failed"    
    print (len(globals()['list_summaryTable_payments']))

    #insert to azureDB
    list_dfAttention = []      
    try:
        #start cleaning/changing dType of data
        #summary dont need to drop because it is made in the code
        df_meli_newTokens = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_response_newTokens'], sep= '_'))
        
        df_meli_payments_summary = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_summaryTable_payments'], sep= '_'))
        df_meli_shipments_summary = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_summaryTable_shipments'], sep= '_'))
        df_meli_reputation_summary = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_summaryTable_reputation'], sep= '_'))
        

        #drop some columns that contains lists #temporary - rawdata
        df_meli_payments_raw = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_rawTable_payments'], sep= '_'))
        df_meli_payments_raw = df_removeLists(df_meli_payments_raw)
        df_meli_shipments_raw = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_rawTable_shipments'], sep = '_'))
        df_meli_shipments_raw = df_removeLists(df_meli_shipments_raw)
        df_meli_reputation_raw = pd.DataFrame.from_dict(pd.json_normalize(globals()['list_rawTable_reputation'], sep = '_'))
        df_meli_reputation_raw = df_removeLists(df_meli_reputation_raw)
        
        df_meli_orders_raw = pd.DataFrame.from_dict(globals()['list_rawTable_orders'])
        
        df_meli_shipments_summary, list_dfAttention = fCorrectTypes(df_meli_shipments_summary, globals()['util'][file]["columnsType_dict"]["meli_shipments_summary"], list_dfAttention)
        df_meli_shipments_summary = df_meli_shipments_summary.drop_duplicates()
        df_meli_payments_summary, list_dfAttention = fCorrectTypes(df_meli_payments_summary, globals()['util'][file]["columnsType_dict"]["meli_payments_summary"], list_dfAttention)
        df_meli_payments_summary = df_meli_payments_summary.drop_duplicates()
        
        df_meli_payments_raw, list_dfAttention = fCorrectTypes(df_meli_payments_raw, globals()['util'][file]["columnsType_dict"]["meli_payments_raw"], list_dfAttention)
        df_meli_payments_raw = df_meli_payments_raw.drop_duplicates()
        df_meli_shipments_raw, list_dfAttention = fCorrectTypes(df_meli_shipments_raw, globals()['util'][file]["columnsType_dict"]["meli_shipments_raw"], list_dfAttention)
        df_meli_shipments_raw = df_meli_shipments_raw.drop_duplicates()
        df_meli_reputation_raw, list_dfAttention = fCorrectTypes(df_meli_reputation_raw, globals()['util'][file]["columnsType_dict"]["meli_reputation_raw"], list_dfAttention)
        df_meli_reputation_raw = df_meli_reputation_raw.drop_duplicates()

        if len(globals()['list_summaryTable_shipments']) > 0:
            try:
                #insert into AzureDB the main df
                print (f'{file} starting mainInsertion time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
                globals()['mainInsertTime'] = datetime.now()

                df_meli_newTokens.to_sql(utilities_load[file]["resultSuccessTable"]["meli_newTokens"], engine_azure, if_exists='append', index=False)
                
                #df_meli_shipments_summary.to_sql(utilities_load[file]["resultSuccessTable"]["meli_shipments_summary"], engine_azure, if_exists='replace', index=False)
                #df_meli_payments_summary.to_sql(utilities_load[file]["resultSuccessTable"]["meli_payments_summary"], engine_azure, if_exists='replace', index=False)
                df_meli_reputation_summary.to_sql(utilities_load[file]["resultSuccessTable"]["meli_reputation_summary"], engine_azure, if_exists='append', index=False)
                #df_meli_payments_raw.to_sql(utilities_load[file]["resultSuccessTable"]["meli_payments_raw"], engine_azure, if_exists='replace', index=False)
                #df_meli_shipments_raw.to_sql(utilities_load[file]["resultSuccessTable"]["meli_shipments_raw"], engine_azure, if_exists='replace', index=False)
                df_meli_reputation_raw.to_sql(utilities_load[file]["resultSuccessTable"]["meli_reputation_raw"], engine_azure, if_exists='append', index=False)
                
                #upsert information
                df_upsert(df_meli_shipments_summary, utilities_load[file]["resultSuccessTable"]["meli_shipments_summary"], engine_azure, match_columns= ['result_shipment_id', 'result_client_id', 'result_order_id'])
                df_upsert(df_meli_payments_summary, utilities_load[file]["resultSuccessTable"]["meli_payments_summary"], engine_azure, match_columns= ['result_payment_id', 'result_client_id', 'result_order_id'])
                df_upsert(df_meli_payments_raw, utilities_load[file]["resultSuccessTable"]["meli_payments_raw"], engine_azure, match_columns= ['id', 'order_id'])
                df_upsert(df_meli_shipments_raw, utilities_load[file]["resultSuccessTable"]["meli_shipments_raw"], engine_azure, match_columns= ['id', 'order_id'])
                
                globals()['mainEndTime'] = datetime.now()
                
                #mark clocks
                globals()['endTime'] = datetime.now()
                globals()['attentionInsertTime'] = datetime.now()
                globals()['attentionEndTime'] = datetime.now()

                #for the main DataFrame
                #successHandle(file= file, additionalInfo= "", runRowNumber= len(df), engine_azure= engine_azure)
                globals()['output'] = "Success"
            except:
                print('nayyyyyy')
                #errorHandle(2, "insertAzureDB", None, file, engine_azure)
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
    file = "datapipeline_meli_functions"
    print (f'{file} start time: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
    globals()['startTime'] = datetime.now()
    
    main(file)
    globals()['endTime'] = datetime.now()
    print('%s: done with the output: %s, runtime %s' %(file, globals()['output'], (globals()['endTime'] - globals()['startTime']).total_seconds()))


