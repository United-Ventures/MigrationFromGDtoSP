import traceback
from datetime import datetime, timedelta
import json
import pandas as pd
import requests
import time
from ZConnector import *
from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations, Record
from zcrmsdk.src.com.zoho.crm.api.related_records import RelatedRecordsOperations
from zcrmsdk.src.com.zoho.crm.api.related_records import RelatedRecordsOperations
from zcrmsdk.src.com.zoho.crm.api import HeaderMap, ParameterMap
from zcrmsdk.src.com.zoho.crm.api.util import Choice, StreamWrapper
from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord
from zcrmsdk.src.com.zoho.crm.api.tags import Tag as ZCRMTag
from zcrmsdk.src.com.zoho.crm.api.layouts import Layout as ZCRMLayout
from zcrmsdk.src.com.zoho.crm.api.users import User as ZCRMUser
from zcrmsdk.src.com.zoho.crm.api.users import *
import mysql.connector
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import pickle
def saveZohoCall(moduleName, zohoId, recordObject):
    dateNow = datetime.now()

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="unitedventures"
    )
    crs = mydb.cursor()

    dataPickle = pickle.dumps(recordObject)  # Pickling the object

    stmt = "INSERT INTO ZohoData (zohoId, date_added, record_object, module_name) VALUES (%s, %s, %s, %s)"
    vals = (int(zohoId), dateNow, dataPickle, moduleName)
    crs.execute(stmt,vals)
    mydb.commit()
    crs.close()
    mydb.close()

def getZohoCall(moduleName, zohoId):

    dateBound = datetime.now()
    bound = 15
    finalBound = dateBound - timedelta(hours=0, minutes=0, seconds=bound)

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="unitedventures"
    )
    crs = mydb.cursor()
    stmt = "SELECT * FROM ZohoData WHERE (zohoId=%s AND module_name=%s AND date_added>%s)"
    vals = (int(zohoId),moduleName,finalBound)
    crs.execute(stmt,vals)
    records = crs.fetchall()
    crs.close()
    mydb.close()
    print("here")
    try:
        return pickle.loads(records[0][3])
    except Exception:
        print(traceback.format_exc())
    return None


SDKInitializer.initialize()
def ResponseDiagnostics(response):
    if response is not None:
        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):
                        # Get the Status
                        print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        print("Code: " + action_response.get_code().get_value())

                        print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            print(key + ' : ' + str(value))

                        # Get the Message
                        print("Message: " + action_response.get_message().get_value())

                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):
                        # Get the Status
                        print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        print("Code: " + action_response.get_code().get_value())

                        print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            print(key + ' : ' + str(value))

                        # Get the Message
                        print("Message: " + action_response.get_message().get_value())

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())
def RecordPrint(record):
    # Get the ID of each Record
    print("Record ID: ")
    print(record.get_id())
    # Get the createdBy User instance of each Record
    created_by = record.get_created_by()
    # Check if created_by is not None
    if created_by is not None:
        # Get the Name of the created_by User
        print("Record Created By - Name: " , created_by.get_name())
        # Get the ID of the created_by User
        print("Record Created By - ID: " , str(created_by.get_id()))
        # Get the Email of the created_by User
        print("Record Created By - Email: " + created_by.get_email())

    # Get the CreatedTime of each Record
    print("Record CreatedTime: " , str(record.get_created_time()))

    if record.get_modified_time() is not None:
        # Get the ModifiedTime of each Record
        print("Record ModifiedTime: " , str(record.get_modified_time()))

    # Get the modified_by User instance of each Record
    modified_by = record.get_modified_by()

    # Check if modified_by is not None
    if modified_by is not None:
        # Get the Name of the modified_by User
        print("Record Modified By - Name: " , modified_by.get_name())

        # Get the ID of the modified_by User
        print("Record Modified By - ID: " , modified_by.get_id())

        # Get the Email of the modified_by User
        print("Record Modified By - Email: " , modified_by.get_email())

    print('Record KeyValues: ')

    key_values = record.get_key_values()

    for key_name, value in key_values.items():

        if isinstance(value, list):
            print("Record KeyName : " + key_name)

            for data in value:
                if isinstance(data, dict):
                    for dict_key, dict_value in data.items():
                        print(dict_key + " : " + str(dict_value))

                else:
                    print(str(data))

        elif isinstance(value, dict):
            print("Record KeyName : " + key_name + " -  Value : ")

            for dict_key, dict_value in value.items():
                print(dict_key + " : " + str(dict_value))

        else:
            print("Record KeyName : " + key_name + " -  Value : " + str(value))
def PerformQuery(queryInstructions):
    from zcrmsdk.src.com.zoho.crm.api.query import QueryOperations
    """
    This method is used to get records from the module through a COQL query.
    """
    from zcrmsdk.src.com.zoho.crm.api.query import BodyWrapper
    from zcrmsdk.src.com.zoho.crm.api.query import ResponseWrapper, APIException
    returnRecords = []
    # Get instance of QueryOperations Class
    query_operations = QueryOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    body_wrapper = BodyWrapper()
    select_query = queryInstructions

    # Set the select_query to BodyWrapper instance
    body_wrapper.set_select_query(select_query)
    # Call get_records method that takes BodyWrapper instance as parameter
    response = query_operations.get_records(body_wrapper)

    if response is not None:
        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))
        if response.get_status_code() in [204, 304]:
            print('No Content' if response.get_status_code() == 204 else 'Not Modified')
            return

        # Get object from response
        response_object = response.get_object()
        if response_object is not None:
            # Check if expected ResponseWrapper instance is received.
            if isinstance(response_object, ResponseWrapper):
                # Get the list of obtained Record instances
                record_list = response_object.get_data()
                for record in record_list:
                    returnRecords.append(record)
                return returnRecords
            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())
                # Get the Code
                print("Code: " + response_object.get_code().get_value())
                print("Details")
                # Get the details dict
                details = response_object.get_details()
                for key, value in details.items():
                    print(key + ' : ' + str(value))
                # Get the Message
                print("Message: " + response_object.get_message().get_value())

def GetCompanyDrive(companyId):
    """ Retrieves the Drive folder id for the company provided.

    """
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations, APIException, ResponseWrapper
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            if isinstance(response_object, ResponseWrapper):
                record_list = response_object.get_data()
                for record in record_list:
                    return record.get_key_value('gdriveextension__Drive_Folder_ID')
    return None
def GetKeyFounders(companyId):
    """ Retreives the id of the user.


    :param companyId:
    :return:
    """
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    allKeyFoundersIds = []
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                allKeyFounders = record.get_key_value('Founders_Key_managemen')
                for founder in allKeyFounders:
                    allKeyFoundersIds.append(founder.get_id())
    return allKeyFoundersIds
def GetCompanyOwnerName(companyId):
    """ Retreives the id of the user.


    :param companyId:
    :return:
    """
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                return record.get_key_value('Owner').get_key_value("name")
    return None
def GetCompanyOwner(companyId):
    """ Retreives the id of the user.


    :param companyId:
    :return:
    """
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                return record.get_key_value('Owner').get_id()
    return None
def GetUserCompaniesGDId(ownerId):
    GDIds = []
    ownerId=str(ownerId)
    allRecords = PerformQuery("select gdriveextension__Drive_Folder_ID from Accounts where Owner="+ownerId+" limit 200")
    if allRecords is not None:
        for elem in allRecords:
            print(elem.get_key_value("gdriveextension__Drive_Folder_ID"))
            GDIds.append(elem.get_key_value("gdriveextension__Drive_Folder_ID"))
    return GDIds
def GetVerticalGDId(vertical):
    GDIds = []
    ownerId=str(vertical)
    allRecords = PerformQuery("select gdriveextension__Drive_Folder_ID from Accounts where Final_Vertical=\""+vertical+"\" limit 200")
    if allRecords is not None:
        for elem in allRecords:
            print(elem.get_key_value("gdriveextension__Drive_Folder_ID"))
            GDIds.append(elem.get_key_value("gdriveextension__Drive_Folder_ID"))
    return GDIds
def GetVertical(companyId):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                if record.get_key_value('Final_Vertical') is not None:
                    return record.get_key_value('Final_Vertical').get_value()
                else:
                    return None
    return None
def GetGDId(companyId):
    companyId = int(companyId)
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                if record.get_key_value('gdriveextension__Drive_Folder_ID') is not None:
                    return record.get_key_value('gdriveextension__Drive_Folder_ID')
                else:
                    return None
    return None
def FindMatchingKey(value, keyMappings):
    for keyVal in keyMappings.keys():
        if (value in keyMappings[keyVal]) or (value == keyVal) or (value.upper() == keyVal.upper()):
            return keyVal
    return value
def ChangeVertical(id, value):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    from zcrmsdk.src.com.zoho.crm.api.record import BodyWrapper
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # List to hold Record instances
    records_list = []
    # Get instance of Record Class
    record = ZCRMRecord()
    # Value to Record's fields
    record.add_key_value('Final_Vertical', Choice(value))
    # Add Record instance to the list
    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    response = record_operations.update_record(id, "Accounts", request)
    print(response.get_status_code())
def GetDescription(id):
        from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
        record_operations = RecordOperations()
        param_instance = ParameterMap()
        response = record_operations.get_record(id,
                                                "Accounts",
                                                param_instance)
        if response is not None:
            response_object = response.get_object()
            if response_object is not None:
                record_list = response_object.get_data()
                for record in record_list:
                    if record.get_key_value('Description') is not None:
                        return record.get_key_value('Description')
                    else:
                        return None
        return None
def getCompanyWebsite(zohoId):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(zohoId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                if record.get_key_value('Website') is not None:
                    return record.get_key_value('Website')
                else:
                    return None
    return None
def SetShortDescription(id, value):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # List to hold Record instances
    records_list = []
    # Get instance of Record Class
    record = ZCRMRecord()
    # Value to Record's fields
    record.add_key_value('Short_Description', value)
    # Add Record instance to the list
    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    response = record_operations.update_record(id, "Accounts", request)

def updateRecord(module, id, record):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    from zcrmsdk.src.com.zoho.crm.api.record import BodyWrapper
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # List to hold Record instances
    records_list = []
    # Add Record instance to the list
    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    response = record_operations.update_record(id, module, request)

def SetRecordField(module, id, fName, value):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    from zcrmsdk.src.com.zoho.crm.api.record import BodyWrapper
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # List to hold Record instances
    records_list = []
    # Get instance of Record Class
    record = ZCRMRecord()
    # Value to Record's fields
    record.add_key_value(fName, value)
    # Add Record instance to the list
    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    response = record_operations.update_record(id, module, request)

    print(response.get_status_code())

def UpdateAllShortDescriptions(allIds):
    for elem in allIds:
        currentDescription = GetDescription(elem)
        if currentDescription is not None:
            shortDescription = (currentDescription[:245] + ' ...') if len(currentDescription) > 245 else currentDescription
            SetShortDescription(elem,shortDescription)
            print("New Short Description: ", shortDescription)
def GetUVFollowers(id):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(id,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                if record.get_key_value('Str_UV_Followers') is not None:
                    return record.get_key_value('Str_UV_Followers').get_value()
                else:
                    return None
    return None
def getCompleteRecord(moduleName, id, cache=False):

    # if cache:
    #     try:
    #         cachedRecord = getZohoCall(moduleName, id)
    #         if cachedRecord is not None:
    #             print("from DB")
    #             return cachedRecord
    #     except Exception:
    #         print(traceback.format_exc())
    #         pass

    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(id,
                                            moduleName,
                                            param_instance)
    print(response)
    if response is not None:
        response_object = response.get_object()
        print(response_object)
        if response_object is not None:
            record_list = response_object.get_data()
            print(record_list)
            if len(record_list)>0:
                # saveZohoCall(moduleName, id, record_list[0])
                return record_list[0]
    return None
def CompaniesWithVerticals():
    allNN = PerformQuery("SELECT id,Final_Vertical FROM Accounts where Final_Vertical is not null LIMIT 200")
    allIds = []
    for elem in allNN:
        allIds.append((elem.get_id(), elem.get_key_value("Final_Vertical")))
    offset = 200
    while len(allNN)>0:
        allNN = PerformQuery("SELECT id,Final_Vertical FROM Accounts where Final_Vertical is not null LIMIT 200 OFFSET " + str(offset))
        if allNN is None:
            allNN =[]
        for elem in allNN:
            allIds.append((elem.get_id(), elem.get_key_value("Final_Vertical")))
        offset+=199
    return allIds

def saveQueryCall():
    return None

def QueryFetchAll(queryText):
    try:
        allNN = PerformQuery(queryText +" LIMIT 200")
        allIds = []
        for elem in allNN:
            allIds.append(elem)
        offset = 200
        while len(allNN)>0:
            allNN = PerformQuery(queryText + " LIMIT 200 OFFSET " + str(offset))
            if allNN is None:
                allNN =[]
            for elem in allNN:
                allIds.append(elem)
            offset+=199


        return allIds
    except:
        return []
def CompaniesWithDescription():
    allNN = PerformQuery("SELECT id,Description FROM Accounts where Description is not null")
    allIds = {}
    for elem in allNN:
        allIds[elem.get_id()] = elem.get_key_value("Description")
    return allIds
def CompaniesWithWeb():
    allNN = PerformQuery("SELECT id,Website,Final_Vertical FROM Accounts where Website is not null LIMIT 200")
    allIds = []
    for elem in allNN:
        allIds.append((elem.get_id(), elem.get_key_value("Final_Vertical"), elem.get_key_value("Website")))
    offset = 200
    while len(allNN)>0:
        allNN = PerformQuery("SELECT id,Website,Final_Vertical FROM Accounts where Website is not null LIMIT 200 OFFSET " + str(offset))
        if allNN is None:
            allNN =[]
        for elem in allNN:
            allIds.append((elem.get_id(), elem.get_key_value("Final_Vertical"), elem.get_key_value("Website")))
        offset+=199
    return allIds
def allLeads():
    allIds = []
    allLeads = PerformQuery("SELECT id FROM Leads where (OUT_AI_Processed is null) OR (OUT_AI_Processed = \"Processing\")")
    if allLeads is None:
        return allIds
    for elem in allLeads:
        allIds.append(elem.get_id())
    return allIds
def getFieldDetails(moduleName):

    fieldsMap = []

    from zcrmsdk.src.com.zoho.crm.api.fields import FieldsOperations
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap
    # Get instance of FieldsOperations Class that takes module_api_name as parameter
    fields_operations = FieldsOperations(moduleName)

    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Possible parameters for get_fields operation
    # param_instance.add(GetFieldsParam.type, "Unused")

    # Call get_fields method that takes paramInstance as parameter
    response = fields_operations.get_fields(param_instance)
    response_object = response.get_object()
    for field in response_object.get_fields():
        currentDescriptor = {}
        # Get the ID of each Field
        if (True):
            print(field.get_id())
            print(field.get_field_label())
            currentDescriptor["name"] = field.get_field_label()
            pick_list_values = field.get_pick_list_values()
            allOptions = []

            if pick_list_values is not None:
                for pick_list_value in pick_list_values:
                    # Get the DisplayValue of each PickListValue
                    print(pick_list_value.get_actual_value())
                    allOptions.append(pick_list_value.get_actual_value())
            # Get the SystemMandatory of each Field
            print("Field SystemMandatory: " + str(field.get_system_mandatory()))

            # Get the Webhook of each Field
            print("Field Webhook: " + str(field.get_webhook()))
            currentDescriptor["type"] = field.get_json_type()
            currentDescriptor["api"] = field.get_api_name()
            print(field.get_webhook())
            # Get the JsonType of each Field
            print("Field JsonType: " + str(field.get_json_type()))

            currentDescriptor["options"] = allOptions
            fieldsMap.append(currentDescriptor)
    return fieldsMap
def CompaniesByVerticals(vertical):
    allNN = PerformQuery("SELECT id,Final_Vertical FROM Accounts where Final_Vertical=\""+vertical+"\" LIMIT 200")
    allIds = []
    if allNN is None:
        return []
    for elem in allNN:
        allIds.append(elem.get_id())
    print(allIds)
    offset = 200
    while len(allNN)>0:
        allNN = PerformQuery("SELECT id,Final_Vertical FROM Accounts where Final_Vertical=\""+vertical+"\" LIMIT 200 OFFSET " + str(offset))
        if allNN is None:
            allNN =[]
        for elem in allNN:
            allIds.append(elem.get_id())
        offset+=199
    return allIds
def GetUserCompaniesVerticals(ownerId):
    print("HERE")
    GDIds = []
    ownerId=str(ownerId)
    allRecords = QueryFetchAll("select Final_Vertical from Accounts where Owner="+ownerId)
    if allRecords is not None:
        for elem in allRecords:
            GDIds.append(elem.get_key_value('Final_Vertical'))
    return GDIds
def substituteVertical(currentVertical, newVertical):
    """ substitute a vertical for another.
    """

    allCompanies = CompaniesByVerticals(currentVertical)
    for elem in allCompanies:
        ChangeVertical(elem, newVertical)
def CreateRecord(moduleName, recordObject):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations, APIException, ResponseWrapper, BodyWrapper
    record_operations = RecordOperations()
    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()
    # List to hold Record instances
    records_list = []
    # Get instance of Record Class
    records_list.append(recordObject)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    header_instance = HeaderMap()
    response = record_operations.create_records(moduleName, request)
    try:
        rObject = response.get_object()
        rsponsesList = rObject.get_data()
        thisResponse = rsponsesList[0]
        print(thisResponse.get_details())
        return int(thisResponse.get_details()["id"])
    except Exception:
        print(traceback.format_exc())
        return None

def userEmailDict():
    usr = {}
    from zcrmsdk.src.com.zoho.crm.api.users import User as ZCRMUser
    from zcrmsdk.src.com.zoho.crm.api.roles import Role
    from zcrmsdk.src.com.zoho.crm.api.profiles import Profile
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap, HeaderMap
    from datetime import datetime
    """
    This method is used to retrieve the users data specified in the API request.
    """
    # Get instance of UsersOperations Class
    users_operations = UsersOperations()
    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Get instance of ParameterMap Class
    header_instance = HeaderMap()

    # Call get_users method that takes ParameterMap instance and HeaderMap instance as parameters
    response = users_operations.get_users(param_instance, header_instance)
    r = response.get_object().get_users()
    for elem in r:
        usr[elem.get_key_value("email").replace(">","").split("<")[1]]=int(elem.get_id())
    return usr
def getUserEmails():
    usr = {}
    from zcrmsdk.src.com.zoho.crm.api.users import User as ZCRMUser
    from zcrmsdk.src.com.zoho.crm.api.roles import Role
    from zcrmsdk.src.com.zoho.crm.api.profiles import Profile
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap, HeaderMap
    from datetime import datetime
    """
    This method is used to retrieve the users data specified in the API request.
    """
    # Get instance of UsersOperations Class
    users_operations = UsersOperations()
    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Get instance of ParameterMap Class
    header_instance = HeaderMap()

    # Call get_users method that takes ParameterMap instance and HeaderMap instance as parameters
    response = users_operations.get_users(param_instance, header_instance)
    r = response.get_object().get_users()
    for elem in r:
        usr[int(elem.get_id())]=elem.get_key_value("email")
    return usr
def get_usersName():
    usr = {}
    from zcrmsdk.src.com.zoho.crm.api.users import User as ZCRMUser
    from zcrmsdk.src.com.zoho.crm.api.roles import Role
    from zcrmsdk.src.com.zoho.crm.api.profiles import Profile
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap, HeaderMap
    from datetime import datetime
    """
    This method is used to retrieve the users data specified in the API request.
    """
    # Get instance of UsersOperations Class
    users_operations = UsersOperations()
    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Get instance of ParameterMap Class
    header_instance = HeaderMap()

    # Call get_users method that takes ParameterMap instance and HeaderMap instance as parameters
    response = users_operations.get_users(param_instance, header_instance)
    r = response.get_object().get_users()
    for elem in r:
        usr[int(elem.get_id())]=elem.get_key_value("full_name")
    return usr

def get_usersEmails():
    usr = {}
    from zcrmsdk.src.com.zoho.crm.api.users import User as ZCRMUser
    from zcrmsdk.src.com.zoho.crm.api.roles import Role
    from zcrmsdk.src.com.zoho.crm.api.profiles import Profile
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap, HeaderMap
    from datetime import datetime
    """
    This method is used to retrieve the users data specified in the API request.
    """
    # Get instance of UsersOperations Class
    users_operations = UsersOperations()
    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Get instance of ParameterMap Class
    header_instance = HeaderMap()

    # Call get_users method that takes ParameterMap instance and HeaderMap instance as parameters
    response = users_operations.get_users(param_instance, header_instance)
    r = response.get_object().get_users()
    for elem in r:

        usr[elem.get_key_value("email")]=int(elem.get_id())
    return usr


def get_users():
    usr = []
    from zcrmsdk.src.com.zoho.crm.api.users import User as ZCRMUser
    from zcrmsdk.src.com.zoho.crm.api.roles import Role
    from zcrmsdk.src.com.zoho.crm.api.profiles import Profile
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap, HeaderMap
    from datetime import datetime
    """
    This method is used to retrieve the users data specified in the API request.
    """
    # Get instance of UsersOperations Class
    users_operations = UsersOperations()
    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Get instance of ParameterMap Class
    header_instance = HeaderMap()

    # Call get_users method that takes ParameterMap instance and HeaderMap instance as parameters
    response = users_operations.get_users(param_instance, header_instance)
    r = response.get_object().get_users()
    for elem in r:
        usr.append(int(elem.get_id()))
    return usr
def GetSubvertical(companyId):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations
    record_operations = RecordOperations()
    param_instance = ParameterMap()
    response = record_operations.get_record(companyId,
                                            "Accounts",
                                            param_instance)
    if response is not None:
        response_object = response.get_object()
        if response_object is not None:
            record_list = response_object.get_data()
            for record in record_list:
                if record.get_key_value('Final_Sub_Vertical') is not None:
                    return record.get_key_value('Final_Sub_Vertical').get_value()
                else:
                    return None
    return None
def updateRevenuesSubform(revenuedDataframe, companyId):
    allYears = revenuedDataframe["Year"].values.tolist()
    allRevenues = revenuedDataframe["Revenue"].values.tolist()
    subformRecords = []
    for index in range(len(allYears)):
        subformRow = ZCRMRecord()
        subformRow.add_key_value("Decimal_1", float(allRevenues[index]))
        subformRow.add_key_value("Reference_Year", str(allYears[index]))
        subformRecords.append(subformRow)
    SetRecordField("Accounts", companyId, "Subform_51", [])
    SetRecordField("Accounts",companyId,"Subform_51", subformRecords)
def search_records(module_api_name, followerName):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations, SearchRecordsParam
    """
    This method is used to search records of a module and print the response.
    :param module_api_name: The API Name of the module to search records.
    """

    """
    example
    module_api_name = "Price_Books"
    """

    # Get instance of RecordOperations Class
    record_operations = RecordOperations()

    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Encoding must be done for parentheses or comma
    param_instance.add(SearchRecordsParam.criteria, '(Str_UV_Followers:contains:{})'.format(followerName))

    header_instance = HeaderMap()

    # Call searchRecords method that takes ParameterMap Instance and moduleAPIName as parameter
    response = record_operations.search_records(module_api_name, param_instance, header_instance)

    allRecordIds= []

    if response is not None:

        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))

        if response.get_status_code() in [204, 304]:
            print('No Content' if response.get_status_code() == 204 else 'Not Modified')
            return

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ResponseWrapper instance is received.
            if isinstance(response_object, ResponseWrapper):

                # Get the list of obtained Record instances
                record_list = response_object.get_data()

                for record in record_list:
                    # Get the ID of each Record
                    allRecordIds.append(record.get_id())

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())
    return allRecordIds

def makeCompanyGoogleDrive(companyId):
    import Zoho.ZConnector
    import requests
    url = "https://www.zohoapis.eu/crm/v2/functions/OpenGoogleDriveCompanyStep0/actions/execute?auth_type=oauth&lead_id=" + str(companyId)


    headers = {
        'Authorization' : "Zoho-oauthtoken "+Zoho.ZConnector.extractToken()
    }

    response = requests.get(url=url, headers=headers)

    print(response.text)

def getLatestCompanyDrive(companyId):
    import Zoho.ZConnector
    import requests
    url = "https://www.zohoapis.eu/crm/v2/Accounts/" + str(companyId)


    headers = {
        'Authorization' : "Zoho-oauthtoken "+Zoho.ZConnector.extractToken()
    }

    response = requests.get(url=url, headers=headers)
    responseJson = response.json()
    print(responseJson)
    return responseJson["data"][0]["gdriveextension__Drive_Folder_ID"]



def uploadFileToField(fPath):
    from zcrmsdk.src.com.zoho.crm.api.file import FileOperations,UploadFilesParam,GetFileParam, BodyWrapper
    from zcrmsdk.src.com.zoho.crm.api import ParameterMap
    from zcrmsdk.src.com.zoho.crm.api.util import StreamWrapper
    """
    This method is used to upload files and print the response.
    """

    # Get instance of FileOperations Class
    file_operations = FileOperations()

    # Get instance of ParameterMap Class
    param_instance = ParameterMap()

    # Possible parameters of UploadFile operation
    param_instance.add(UploadFilesParam.type, "inline")

    # Get instance of FileBodyWrapper Class that will contain the request body
    request = BodyWrapper()

    """
    StreamWrapper can be initialized in any of the following ways

    * param 1 -> fileName 
    * param 2 -> Read Stream.
    """
    # stream_wrapper = StreamWrapper(stream=open(absolute_file_path, 'rb'))

    """
    * param 1 -> fileName
    * param 2 -> Read Stream
    * param 3 -> Absolute File Path of the file to be attached
    """

    stream_wrapper1 = StreamWrapper(file_path=fPath)

    # stream_wrapper2 = StreamWrapper(file_path="/Users/username/sdk_logs.txt")
    #
    # stream_wrapper3 = StreamWrapper(file_path="/Users/username/sdk_logs.txt")

    # Set list of files to the BodyWrapper instance
    request.set_file([stream_wrapper1])  # , stream_wrapper2, stream_wrapper3])

    # Call upload_files method that takes BodyWrapper instance and ParameterMap instance as parameter.
    response = file_operations.upload_files(request, param_instance)

    if response is not None:
        # Get the status code from response
        print('Status Code: ' + str(response.get_status_code()))

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:

            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):
                        # Get the Status
                        print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        print("Code: " + action_response.get_code().get_value())

                        print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            print(key + ' : ' + str(value))

                        # Get the Message
                        print("Message: " + action_response.get_message().get_value())

                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):
                        # Get the Status
                        print("Status: " + action_response.get_status().get_value())

                        # Get the Code
                        print("Code: " + action_response.get_code().get_value())

                        print("Details")

                        # Get the details dict
                        details = action_response.get_details()

                        for key, value in details.items():
                            print(key + ' : ' + str(value))

                        # Get the Message
                        print("Message: " + action_response.get_message().get_value())

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):
                # Get the Status
                print("Status: " + response_object.get_status().get_value())

                # Get the Code
                print("Code: " + response_object.get_code().get_value())

                print("Details")

                # Get the details dict
                details = response_object.get_details()

                for key, value in details.items():
                    print(key + ' : ' + str(value))

                # Get the Message
                print("Message: " + response_object.get_message().get_value())

def makeCompany(name, web, vertical, country):
    r = ZCRMRecord()
    r.add_key_value("Account_Name", name)
    r.add_key_value("Website", web)
    r.add_key_value("Final_Vertical", Choice(vertical))
    r.add_key_value("Country", Choice(country))
    competitorZohoId = CreateRecord("Accounts", r)
    return competitorZohoId


# RelatedLists
# allDeals = QueryFetchAll("select id from Deals where gid is not null")
# for deal in allDeals:
#     dId = deal.get_id()
#     print(dId)
#     dObj = getCompleteRecord("Deals", dId)
#     print(dObj.get_key_value("gid"))


# r = ZCRMRecord()
# r.add_key_value("Account_Name", "NEW-ERA SPA")
# r.add_key_value("Website", "someweb.com")
# zId = CreateRecord("Accounts", r)
# print(zId)


# allDeals = QueryFetchAll("select id from Deals where gid={}")
# thisDeal = allDeals[0] if len(allDeals)>0 else None
# if thisDeal is not None:
#     SetRecordField("Deals", thisDeal.get_id(), "sid", "XYZ")


# GOOGLE_FOLDER_ID = "1BvRvLq0jzJwxTPVkXDkCWLNiCS46fFej"   # ID della cartella di prova

# rec = ZCRMRecord()
# rec.add_key_value("Account_Name", "Test Migration Folder 2")          # 👈 Campo obbligatorio
# rec.add_key_value("gdriveextension__Drive_Folder_ID", GOOGLE_FOLDER_ID)

# new_id = CreateRecord("Accounts", rec)
# print(f"Creato account di prova con id Zoho {new_id}")

# ---------------------------------------------------------------------------
# # 1. Imposta il GID dell'Account
# # ---------------------------------------------------------------------------
# COMPANY_GID = "1BvRvLq0jzJwxTPVkXDkCWLNiCS46fFej"   # ← quello che hai appena creato

# # ---------------------------------------------------------------------------
# # 2. Trova i Deal che hanno quel GID
# # ---------------------------------------------------------------------------
# query = (
#     f'SELECT id, gid FROM Deals '
#     f'WHERE gid = "{COMPANY_GID}"'
# )
# deals = PerformQuery(query) or []

# if not deals:
#     print("⚠️  Nessun Deal con gid =", COMPANY_GID)
# else:
#     for d in deals:
#         deal_id = d.get_id()
#         deal_gid = d.get_key_value("gid")
#         print(f"Deal ID Zoho: {deal_id}  –  gid: {deal_gid}")

#         # -------------------------------------------------------------------
#         # 3. (opzionale) Recupera l'intero record per leggere altri campi
#         # -------------------------------------------------------------------
#         deal_obj = getCompleteRecord("Deals", deal_id)
#         print("Account lookup:", deal_obj.get_key_value("Account_Name"))
#         print("Stage:",        deal_obj.get_key_value("Stage"))
#         print("Data chiusura:", deal_obj.get_key_value("Closing_Date"))

# DEAL_ID = 327256000067933003     # ← l’ID che mi hai dato

# deal = getCompleteRecord("Deals", DEAL_ID)

# # Stampa i key/value per individuare il campo che contiene il G-ID
# for k, v in deal.get_key_values().items():
#     print(f"{k:25} → {v}")


# # ---------------------------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------------------------
# csv_path = "/Users/gfiacconiuv/Desktop/newera/migration_audit.csv"  # <-- path corretto al tuo CSV
# dry_run = False  # <-- ORA ESEGUIAMO L'UPDATE VERO

# # ---------------------------------------------------------------------------
# # LOGGING helper
# # ---------------------------------------------------------------------------
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # ---------------------------------------------------------------------------
# # START WORK
# # ---------------------------------------------------------------------------
# df = pd.read_csv(csv_path)

# module_name = "Deals"
# google_field = "gid"

# for idx, row in df.iterrows():
#     google_id = row["Google ID"].strip()
#     sharepoint_link = row["SharePoint Link"].strip()

#     if not google_id or not sharepoint_link:
#         log(f"⚠️ Riga {idx} mancante di dati, saltata")
#         continue

#     query = f'SELECT id FROM {module_name} WHERE {google_field} = "{google_id}"'
    
#     try:
#         records = PerformQuery(query)
#     except Exception as e:
#         log(f"⚠️ Errore nella query per {google_id}: {e}")
#         continue

#     if not records:
#         log(f"❌ Nessun Deal trovato per GID: {google_id}")
#     else:
#         for record in records:
#             deal_id = record.get_id()
#             log(f"✅ Trovato Deal ID: {deal_id} per GID: {google_id}")

#             try:
#                 deal_obj = getCompleteRecord(module_name, deal_id)
#                 current_value = deal_obj.get_key_value(google_field)
#                 log(f"    - Valore corrente: {current_value}")
#                 log(f"    - Nuovo valore:    {sharepoint_link}")

#                 if not dry_run:
#                     SetRecordField(module_name, deal_id, google_field, sharepoint_link)
#                     log(f"    → ✅ Record aggiornato su Zoho")

#             except Exception as e:
#                 log(f"⚠️ Errore durante aggiornamento per Deal ID {deal_id}: {traceback.format_exc()}")

#     time.sleep(0.2)

# log("🚀 FINITO.")

# # ---------------------------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------------------------
# google_id = "1RnSI4p7K8N-8WFVGJEwcNP9FWb9V7ayo"  # <-- qui metti a mano il GID che vuoi cercare

# # ---------------------------------------------------------------------------
# # LOGGING helper
# # ---------------------------------------------------------------------------
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # ---------------------------------------------------------------------------
# # LOGICA
# # ---------------------------------------------------------------------------
# module_name = "Deals"
# google_field = "gid"
# company_google_field = "gdriveextension__Drive_Folder_ID"

# # Costruisci la query COQL
# query = f'SELECT id, Account_Name FROM {module_name} WHERE {google_field} = "{google_id}"'

# try:
#     records = PerformQuery(query)
# except Exception as e:
#     log(f"⚠️ Errore nella query per {google_id}: {e}")
#     records = []

# if not records:
#     log(f"❌ Nessun Deal trovato per GID: {google_id}")
# else:
#     for record in records:
#         deal_id = record.get_id()
#         log(f"✅ Trovato Deal ID: {deal_id} per GID: {google_id}")

#         try:
#             deal_obj = getCompleteRecord(module_name, deal_id)
            
#             account = deal_obj.get_key_value("Account_Name")
#             if account:
#                 company_id = account.get_id()
#                 company_name = account.get_key_value("name")
#                 log(f"🔗 Company associata trovata: ID={company_id}, Nome={company_name}")

#                 # Ora recuperiamo il GID della company
#                 company_obj = getCompleteRecord("Accounts", company_id)
#                 company_gid = company_obj.get_key_value(company_google_field)
#                 log(f"    - GID Company attuale: {company_gid}")
#             else:
#                 log("⚠️ Nessuna Company associata al Deal")

#         except Exception as e:
#             log(f"⚠️ Errore durante il recupero dettagli: {traceback.format_exc()}")



#########RITORNA INDIETRO IL GID ORIGINALE DI UN DEAL
# # ---------------------------------------------------------------------------
# # Config
# deal_id = 327256000067933003
# field_api_name = "gid"
# sharepoint_link_attuale = "https://unitedventuressgr.sharepoint.com/sites/Etabeta-ERP/Shared%20Documents/Young/01.%20Presentation%20deck%20e%20business%20plan/2025%2006%2016%20Deal"
# google_gid_originale = "1RnSI4p7K8N-8WFVGJEwcNP9FWb9V7ayo"

# # Logger
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # Inizio procedura
# log("🔄 Inizio ripristino valore originale")

# # Recuperiamo il record esistente usando la tua getCompleteRecord()
# deal_obj = getCompleteRecord("Deals", deal_id)
# current_value = deal_obj.get_key_value(field_api_name)

# log(f"📌 Valore attuale nel campo '{field_api_name}': {current_value}")

# # Verifica
# if current_value == sharepoint_link_attuale:
#     log("✅ Valore attuale corrisponde a SharePoint link, procedo al ripristino...")

#     # Esegui l'update usando SetRecordField, come in tutto il tuo progetto
#     SetRecordField("Deals", deal_id, field_api_name, google_gid_originale)

#     log("🎯 Ripristino completato ✅")
# else:
#     log("⚠️ Il valore attuale NON corrisponde, ripristino annullato.")

# ---------------------------------------------------------------------------
# Config
# company_id = 327256000067923005
# field_api_name = "gdriveextension__Drive_Folder_ID"
# sharepoint_link_attuale = "https://unitedventuressgr.sharepoint.com/sites/Etabeta-ERP/Shared%20Documents/Young"
# google_gid_originale = "1BvRvLq0jzJwxTPVkXDkCWLNiCS46fFej"

# # Logger
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # Inizio procedura
# log("🔄 Inizio ripristino valore originale per la Company")

# # Recuperiamo il record esistente usando la tua getCompleteRecord()
# company_obj = getCompleteRecord("Accounts", company_id)
# current_value = company_obj.get_key_value(field_api_name)

# log(f"📌 Valore attuale nel campo '{field_api_name}': {current_value}")

# # Verifica
# if current_value == sharepoint_link_attuale:
#     log("✅ Valore attuale corrisponde a SharePoint link, procedo al ripristino...")

#     # Esegui l'update usando SetRecordField
#     SetRecordField("Accounts", company_id, field_api_name, google_gid_originale)

#     log("🎯 Ripristino completato ✅")
# else:
#     log("⚠️ Il valore attuale NON corrisponde, ripristino annullato.")

# CONFIG
# deal_id = 327256000067933003
# deal_field = "gid"
# expected_gid_deal = "1RnSI4p7K8N-8WFVGJEwcNP9FWb9V7ayo"

# company_id = 327256000067923005
# company_field = "gdriveextension__Drive_Folder_ID"
# expected_gid_company = "1BvRvLq0jzJwxTPVkXDkCWLNiCS46fFej"

# # Logger
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # CHECK DEAL
# log("🔎 Verifica Deal...")
# deal_obj = getCompleteRecord("Deals", deal_id)
# current_deal_value = deal_obj.get_key_value(deal_field)

# log(f"Deal {deal_id} - Valore attuale '{deal_field}': {current_deal_value}")

# if current_deal_value == expected_gid_deal:
#     log("✅ Deal OK")
# else:
#     log("❌ Deal NON corrisponde al valore atteso!")

# # CHECK COMPANY
# log("🔎 Verifica Company...")
# company_obj = getCompleteRecord("Accounts", company_id)
# current_company_value = company_obj.get_key_value(company_field)

# log(f"Company {company_id} - Valore attuale '{company_field}': {current_company_value}")

# if current_company_value == expected_gid_company:
#     log("✅ Company OK")
# else:
#     log("❌ Company NON corrisponde al valore atteso!")


# # ---------------------------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------------------------
# csv_path = "/Users/gfiacconiuv/Desktop/newera/migration_full.csv"  # <-- path corretto del tuo CSV unificato
# dry_run = False  # <-- ORA ESEGUIAMO L'UPDATE VERO

# # ---------------------------------------------------------------------------
# # LOGGING helper
# # ---------------------------------------------------------------------------
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # ---------------------------------------------------------------------------
# # LOAD CSV as dictionary for fast lookup
# # ---------------------------------------------------------------------------
# df = pd.read_csv(csv_path)
# mapping_dict = dict(zip(df["Google ID"].astype(str), df["SharePoint Link"]))

# # ---------------------------------------------------------------------------
# # STEP 1: Aggiorna i DEALS
# # ---------------------------------------------------------------------------
# log("🚀 Inizio aggiornamento DEALS")

# # prendiamo solo i GID di tipo Deal
# deal_gids = [
#     gid for gid, link in mapping_dict.items()
#     if gid.startswith("1")  # filtro generico, opzionale
# ]

# for gid in deal_gids:
#     sharepoint_link = mapping_dict.get(gid)
#     module_name = "Deals"
#     google_field = "gid"

#     query = f'SELECT id, Account_Name FROM {module_name} WHERE {google_field} = "{gid}"'
    
#     try:
#         records = PerformQuery(query)
#     except Exception as e:
#         log(f"⚠️ Errore nella query per {gid}: {e}")
#         continue

#     if not records:
#         log(f"❌ Nessun Deal trovato per GID: {gid}")
#     else:
#         for record in records:
#             deal_id = record.get_id()
#             log(f"✅ Trovato Deal ID: {deal_id} per GID: {gid}")

#             try:
#                 deal_obj = getCompleteRecord(module_name, deal_id)
#                 current_value = deal_obj.get_key_value(google_field)
#                 log(f"    - Valore corrente: {current_value}")
#                 log(f"    - Nuovo valore:    {sharepoint_link}")

#                 if not dry_run:
#                     SetRecordField(module_name, deal_id, google_field, sharepoint_link)
#                     log(f"    → ✅ Deal aggiornato su Zoho")

#                 # Prendiamo anche l'Account associato
#                 account = deal_obj.get_key_value("Account_Name")
#                 if account:
#                     account_id = account.get_id()

#                     # Recupera il GID della Company (Account)
#                     account_obj = getCompleteRecord("Accounts", account_id)
#                     company_gid = account_obj.get_key_value("gdriveextension__Drive_Folder_ID")

#                     if company_gid:
#                         company_sharepoint_link = mapping_dict.get(company_gid)
#                         log(f"    ↳ Company associata: ID={account_id}, GID={company_gid}")

#                         if company_sharepoint_link:
#                             log(f"    - Nuovo Company SharePoint Link: {company_sharepoint_link}")
#                             if not dry_run:
#                                 SetRecordField("Accounts", account_id, "gdriveextension__Drive_Folder_ID", company_sharepoint_link)
#                                 log(f"    → ✅ Company aggiornata su Zoho")
#                         else:
#                             log(f"    ⚠️ Nessun SharePoint trovato per il GID Company: {company_gid}")

#             except Exception as e:
#                 log(f"⚠️ Errore durante aggiornamento per Deal ID {deal_id}: {traceback.format_exc()}")

#     time.sleep(0.2)

# log("🎯 FINITO")

# # CONFIG
# deal_id = 327256000067933003  # il Deal ID manuale
# google_field = "gid"
# sid_field = "sid"

# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# log("🔄 Inizio aggiornamento manuale SID")

# # Recuperiamo il record completo usando la tua funzione classica
# deal_obj = getCompleteRecord("Deals", deal_id)
# current_gid_value = deal_obj.get_key_value(google_field)

# log(f"📌 Valore attuale GID: {current_gid_value}")

# # Aggiorniamo il campo SID usando la tua SetRecordField()
# SetRecordField("Deals", deal_id, sid_field, current_gid_value)

# log(f"🎯 Campo SID aggiornato con valore: {current_gid_value}")



# #############################∞FUNZIONA##############################################
# # ---------------------------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------------------------
# csv_path = "/Users/gfiacconiuv/Desktop/newera/migration_full.csv"
# dry_run = False

# # ---------------------------------------------------------------------------
# # LOGGING helper
# # ---------------------------------------------------------------------------
# def log(msg):
#     print(f"{datetime.now().isoformat()} | {msg}")

# # ---------------------------------------------------------------------------
# # LOAD CSV as dictionary for fast lookup
# # ---------------------------------------------------------------------------
# df = pd.read_csv(csv_path)
# mapping_dict = dict(zip(df["Google ID"].astype(str), df["SharePoint Path"]))

# # ---------------------------------------------------------------------------
# # STEP 1: Aggiorna i DEALS
# # ---------------------------------------------------------------------------
# log("🚀 Inizio aggiornamento DEALS")

# deal_gids = [
#     gid for gid, link in mapping_dict.items()
#     if gid.startswith("1")
# ]

# for gid in deal_gids:
#     sharepoint_path = mapping_dict.get(gid)
#     module_name = "Deals"
#     google_field = "gid"
#     sid_field = "sid"  # <-- aggiunto il campo SID

#     query = f'SELECT id, Account_Name FROM {module_name} WHERE {google_field} = "{gid}"'
    
#     try:
#         records = PerformQuery(query)
#     except Exception as e:
#         log(f"⚠️ Errore nella query per {gid}: {e}")
#         continue

#     if not records:
#         log(f"❌ Nessun Deal trovato per GID: {gid}")
#     else:
#         for record in records:
#             deal_id = record.get_id()
#             log(f"✅ Trovato Deal ID: {deal_id} per GID: {gid}")

#             try:
#                 deal_obj = getCompleteRecord(module_name, deal_id)
#                 current_value_sid = deal_obj.get_key_value(sid_field)
#                 log(f"    - SID corrente: {current_value_sid}")
#                 log(f"    - Nuovo SID: {sharepoint_path}")

#                 if not dry_run:
#                     SetRecordField(module_name, deal_id, sid_field, sharepoint_path)
#                     log(f"    → ✅ SID aggiornato su Zoho")

#                 # Aggiorno anche la company come prima
#                 account = deal_obj.get_key_value("Account_Name")
#                 if account:
#                     account_id = account.get_id()

#                     account_obj = getCompleteRecord("Accounts", account_id)
#                     company_gid = account_obj.get_key_value("gdriveextension__Drive_Folder_ID")

#                     if company_gid:
#                         company_sharepoint_path = mapping_dict.get(company_gid)
#                         log(f"    ↳ Company associata: ID={account_id}, GID={company_gid}")

#                         if company_sharepoint_path:
#                             log(f"    - Nuovo Company SharePoint Path: {company_sharepoint_path}")
#                             if not dry_run:
#                                 SetRecordField("Accounts", account_id, "gdriveextension__Drive_Folder_ID", company_sharepoint_path)
#                                 log(f"    → ✅ Company aggiornata su Zoho")
#                         else:
#                             log(f"    ⚠️ Nessun SharePoint Path trovato per il GID Company: {company_gid}")

#             except Exception as e:
#                 log(f"⚠️ Errore durante aggiornamento per Deal ID {deal_id}: {traceback.format_exc()}")

#     time.sleep(0.2)

# log("🎯 FINITO")
# #############################∞FUNZIONA FINE##############################################


import pandas as pd
import time
from datetime import datetime
from typing import List, Dict, Tuple
# ------------------------------------------------------------------------------

CSV_PATH       = "migration_full.csv"  # deve contenere la colonna “Google ID”
MODULE         = "Deals"
GID_FIELD      = "gid"                       # campo Zoho con il Google-ID
SID_FIELD      = "sid"                       # API-name del tuo custom field SID
SLEEP_SEC      = 0.2                         # throttle per sicurezza

# altri campi che vuoi visualizzare (API-name → etichetta di colonna)
FIELDS_TO_READ = {
    SID_FIELD          : "sid attuale",
    "Stage"            : "Stage",
    "Account_Name"     : "Account",          # lookup: Zoho restituisce oggetto
}

def csv_gid_lookup(csv_path: str) -> pd.DataFrame:
    df          = pd.read_csv(csv_path, dtype=str)
    out_rows    = []

    for _, row in df.iterrows():
        gid = (row.get("Google ID") or "").strip()
        if not gid:
            continue                       # riga senza GID → skip

        query = f'''
        SELECT id, {GID_FIELD}, {", ".join(FIELDS_TO_READ)}
        FROM {MODULE}
        WHERE {GID_FIELD} = "{gid}"
        LIMIT 1
        '''
        recs = PerformQuery(query) or []

        if not recs:                       # niente trovato -------------------
            out_rows.append({
                "Google ID": gid,
                "Esito":     "❌ Non trovato",
                **{col: None for col in ["Deal ID", *FIELDS_TO_READ.values()]}
            })
        else:                              # record trovato -------------------
            r   = recs[0]
            rowdata = {
                "Google ID" : gid,
                "Deal ID"   : r.get_id(),
                "Esito"     : "✅ Trovato",
            }

            # per ogni campo richiesto estrai il valore
            for api_name, col_label in FIELDS_TO_READ.items():
                val = r.get_key_value(api_name)
                # se è un lookup (es. Account_Name) restituisce un oggetto
                if hasattr(val, "get_key_value"):        # lookup record
                    val = val.get_key_value("name")
                rowdata[col_label] = val

            out_rows.append(rowdata)

        time.sleep(SLEEP_SEC)

    return pd.DataFrame(out_rows)

def sample_deal_gids(limit: int = 50, offset: int = 0) -> list[tuple]:
    """
    Restituisce fino a `limit` tuple (deal_id, gid, sid) dai Deals
    dove 'gid' è valorizzato (cioè diverso da stringa vuota).
    """
    query = (
        "SELECT id, gid, sid "
        "FROM Deals "
        "WHERE gid != '' "
        f"LIMIT {limit} OFFSET {offset}"
    )

    try:
        records = PerformQuery(query) or []
    except Exception as e:
        print(f"⚠️ Errore durante la query: {e}")
        return []

    return [
        (rec.get_id(),
         rec.get_key_value("gid"),
         rec.get_key_value("sid"))
        for rec in records
    ]

def fetch_deals_with_gid(max_records: int = 50) -> list[tuple]:
    """
    Ritorna al massimo `max_records` Deal che hanno GID valorizzato.
    Ogni tupla è (deal_id, gid, sid correnti).
    Se servono più di 200 record, basta aumentare max_records
    e ciclerà automaticamente con OFFSET.
    """
    results = []
    page_size = 200  # limite massimo COQL
    offset = 0

    while len(results) < max_records:
        # la WHERE usa il trucco `gid != ""` perché
        #  - `IS NOT NULL` non è supportato da COQL
        #  - `gid != null` dà errore di operatore
        query = (
            f'SELECT id, gid, sid FROM Deals '
            f'WHERE gid != "" LIMIT {page_size} OFFSET {offset}'
        )

        records = PerformQuery(query) or []
        if not records:          # niente più righe da leggere
            break

        for rec in records:
            gid_val = rec.get_key_value("gid")
            sid_val = rec.get_key_value("sid")
            # Nel dubbio filtriamo ancora in Python (qualche record
            # con gid="" potrebbe comunque passare se è None)
            if gid_val:
                results.append((rec.get_id(), gid_val, sid_val))

            if len(results) >= max_records:
                break

        offset += page_size      # pagina successiva

    return results

def lookup_deals_from_csv(csv_path: str,
                          gid_col: str = "Google ID",
                          pause: float = 0.2) -> pd.DataFrame:
    """
    • Legge un CSV (colonna 'Google ID').
    • Per ogni G-ID cerca in Zoho *Deals* quel record.
    • Raccoglie: dealId, gid, sid, Stage, Account_Name.
    • NON fa alcun update.

    Ritorna e salva un DataFrame ordinato come 'gid_sid_lookup_<timestamp>.csv'.
    """
    def log(msg: str):                                # logging minimale
        print(f"{datetime.now().isoformat()} | {msg}")

    # ---------- 1. carica il CSV ------------------------------------------------
    df = pd.read_csv(csv_path)
    gids: List[str] = (
        df[gid_col].dropna().astype(str).str.strip().unique().tolist()
    )
    log(f"Letti {len(gids)} G-ID dal file")

    # ---------- 2. ciclo di lookup ---------------------------------------------
    rows: List[Dict[str, str]] = []
    for idx, gid in enumerate(gids, 1):
        # COQL: recuperiamo anche sid, Stage e Account_Name in un colpo solo
        query = (
            "SELECT id, gid, sid, Stage, Account_Name "
            f"FROM Deals WHERE gid = \"{gid}\""
        )
        try:
            records = PerformQuery(query) or []
        except Exception as e:
            log(f"⚠️ Query fallita per {gid}: {e}")
            continue

        if not records:
            rows.append({
                "Google ID": gid,
                "Esito":     "❌ Non trovato",
                "Deal ID":   None,
                "sid":       None,
                "Stage":     None,
                "Account":   None,
            })
        else:
            # in teoria può esserci più di un Deal con lo stesso gid
            for rec in records:
                deal_id = rec.get_id()
                row = {
                    "Google ID": gid,
                    "Esito":     "✅ Trovato",
                    "Deal ID":   deal_id,
                    "sid":       rec.get_key_value("sid"),
                    "Stage":     rec.get_key_value("Stage"),
                    "Account": (
                        rec.get_key_value("Account_Name").get_key_value("name")
                        if rec.get_key_value("Account_Name") else None
                    ),
                }
                rows.append(row)

        time.sleep(pause)   # throttling soft per non saturare l’API


    # ---------- 3. esporta risultato -------------------------------------------
    out_df = pd.DataFrame(rows)
    out_name = f"gid_sid_lookup_{datetime.now():%Y%m%d_%H%M%S}.csv"
    out_df.to_csv(out_name, index=False)
    log(f"✔︎ Risultati salvati in {out_name} ({len(out_df)} righe)")

    return out_df

def lookup_deal_by_gid(
        gid: str,
        deal_module: str = "Deals",
        gid_field: str = "gid",
        sid_field: str = "sid",
        acc_gid_field: str = "gdriveextension__Drive_Folder_ID",
        acc_sid_field: str = "sid"         # <- aggiunto per leggere il SID dell’account
    ) -> dict | None:
    """Cerca un Deal dato un G-ID e ritorna info su Deal + Account associato."""
    from datetime import datetime
    def log(m): print(f"{datetime.now().isoformat()} | {m}")

    # 1️⃣ Deal per G-ID
    q = (f'SELECT id,{gid_field},{sid_field},Stage,Account_Name '
         f'FROM {deal_module} WHERE {gid_field}="{gid}"')
    recs = PerformQuery(q) or []
    if not recs:
        log(f"❌ Nessun Deal con G-ID {gid}")
        return None

    d = recs[0]
    out = {
        "deal_id"    : d.get_id(),
        "deal_gid"   : gid,
        "deal_sid"   : d.get_key_value(sid_field),
        "deal_stage" : d.get_key_value("Stage"),
        "account_id" : None,
        "account_name": None,
        "account_gid": None,
        "account_sid": None,
    }

    # 2️⃣ Account collegato (se c’è)
    acc = d.get_key_value("Account_Name")
    if acc and isinstance(acc, dict):
        acc_id   = acc.get("id")
        acc_name = acc.get("name")
        out.update(account_id=acc_id, account_name=acc_name)

        try:
            acc_obj = getCompleteRecord("Accounts", int(acc_id))
            out["account_gid"] = acc_obj.get_key_value(acc_gid_field)
            out["account_sid"] = acc_obj.get_key_value(acc_sid_field)
        except Exception as e:
            log(f"⚠️ Impossibile leggere Account {acc_id}: {e}")

    return out

def lookup_account_by_gid(gid: str,
                          acc_module: str = "Accounts",
                          acc_gid_field: str = "gdriveextension__Drive_Folder_ID",
                          sid_field: str = "sid",          # se usi un SID anche sugli account
                          deal_module: str = "Deals",      # per recuperare (opz.) i deal collegati
                          deal_gid_field: str = "gid"
                          ) -> dict | None:
    """
    Cerca un ACCOUNT dato il suo G-Drive ID (gid) e, se vuoi,
    elenca i Deal collegati.

    Ritorna un dict con:
        account_id, account_name, account_gid, account_sid
        deals -> lista di dict (deal_id, deal_gid, deal_sid, stage)
    """
    def log(msg: str):
        print(f"{datetime.now().isoformat()} | {msg}")

    # ------------------------------------------------------------------ #
    # 1) QUERY sull'ACCOUNT in base al G-ID                              #
    # ------------------------------------------------------------------ #
    query_acc = (
        f'SELECT id, Account_Name, {acc_gid_field}, {sid_field} '
        f'FROM {acc_module} WHERE {acc_gid_field} = "{gid}"'
    )

    try:
        recs = PerformQuery(query_acc) or []
    except Exception as e:
        log(f"⚠️  Errore COQL account: {e}")
        return None

    if not recs:
        log(f"❌ Nessun Account trovato per GID: {gid}")
        return None

    acc = recs[0]
    acc_id   = acc.get_id()
    acc_name = acc.get_key_value("Account_Name")
    acc_sid  = acc.get_key_value(sid_field)

    result = {
        "account_id"   : acc_id,
        "account_name" : acc_name,
        "account_gid"  : gid,
        "account_sid"  : acc_sid,
        "deals"        : []          # popolata tra poco
    }

    # ------------------------------------------------------------------ #
    # 2) (OPZIONALE) RECUPERA I DEAL COLLEGATI A QUESTO ACCOUNT          #
    # ------------------------------------------------------------------ #
    query_deals = (
        f'SELECT id, {deal_gid_field}, {sid_field}, Stage '
        f'FROM {deal_module} WHERE Account_Name.id = {acc_id}'
    )

    try:
        deals = PerformQuery(query_deals) or []
    except Exception as e:
        log(f"⚠️  Errore COQL deals: {e}")
        deals = []

    for d in deals:
        result["deals"].append({
            "deal_id"   : d.get_id(),
            "deal_gid"  : d.get_key_value(deal_gid_field),
            "deal_sid"  : d.get_key_value(sid_field),
            "stage"     : d.get_key_value("Stage"),
        })

    return result

def set_account_sid(account_id: int,
                    new_sid: str = "prova",
                    sid_field: str = "sid") -> None:
    """
    Scrive `new_sid` nel campo SID dell’Account indicato.

    Parameters
    ----------
    account_id : int
        ID Zoho dell’Account.
    new_sid : str
        Valore da scrivere (default: "prova").
    sid_field : str
        API name del campo SID sul modulo Accounts.
    """
    # usa direttamente la tua utility di update
    SetRecordField("Accounts", account_id, sid_field, new_sid)
    print(f"✔︎ Account {account_id} aggiornato: {sid_field} = '{new_sid}'")

# --------------------------- helper di logging ---------------------------------
def log(msg: str) -> None:
    print(f"{datetime.now().isoformat()} | {msg}")
# --------------------------- funzione principale -------------------------------
def sync_sid_from_csv(
    csv_path: str,
    deal_module: str = "Deals",
    deal_gid_field: str = "gid",
    deal_sid_field: str = "sid",
    acc_module: str = "Accounts",
    acc_gid_field: str = "gdriveextension__Drive_Folder_ID",
    acc_sid_field: str = "sid",
    dry_run: bool = False,
    pause: float = 0.25
) -> None:
    """
    1. legge il CSV ➜ Google ID / SharePoint Path
    2. loop #1: aggiorna **SOLO i Deal** dove gid = Google ID   → set sid
    3. loop #2: aggiorna **SOLO gli Account** dove gdriveextension__… = Google ID → set sid
       (non usa più il lookup dell’Account dal Deal)

    Non tocca mai altri moduli.
    """
    import pandas as pd, time, traceback
    from datetime import datetime

    def log(msg: str):
        print(f"{datetime.now().isoformat()} | {msg}")

    df = pd.read_csv(csv_path).fillna("")
    rows = list(zip(df["Google ID"].astype(str), df["SharePoint Path"].astype(str)))

    #############
    # PASSO 1 - DEAL
    #############
    log("---- PASSO 1: DEAL ----")
    for gid, sp_path in rows:
        if not gid or not sp_path:
            continue

        query = (
            f'SELECT id, {deal_sid_field} FROM {deal_module} '
            f'WHERE {deal_gid_field} = "{gid}"'
        )
        try:
            recs = PerformQuery(query) or []
        except Exception as e:
            log(f"⚠️ COQL Deal {gid}: {e}")
            continue

        if not recs:
            log(f"❌ Deal non trovato (gid={gid})")
            continue

        for r in recs:
            deal_id = r.get_id()
            log(f"✅ Deal {deal_id} trovato – aggiorno sid → {sp_path}")
            if not dry_run:
                SetRecordField(deal_module, deal_id, deal_sid_field, sp_path)
            time.sleep(pause)

    #############
    # PASSO 2 - ACCOUNT
    #############
    log("---- PASSO 2: ACCOUNT ----")
    for gid, sp_path in rows:
        if not gid or not sp_path:
            continue

        query = (
            f'SELECT id FROM {acc_module} '
            f'WHERE {acc_gid_field} = "{gid}"'
        )
        try:
            recs = PerformQuery(query) or []
        except Exception as e:
            log(f"⚠️ COQL Account {gid}: {e}")
            continue

        if not recs:
            log(f"⚠️ Account non trovato (gid={gid})")
            continue

        for r in recs:
            acc_id = r.get_id()
            log(f"✅ Account {acc_id} trovato – aggiorno sid → {sp_path}")
            if not dry_run:
                SetRecordField(acc_module, acc_id, acc_sid_field, sp_path)
            time.sleep(pause)

    log("🎯 FINE SINCRONIZZAZIONE")

#  ─── funzione di base per aggiornare un singolo campo di un record Zoho ───────
def set_record_field(module: str, record_id: int, field_api: str, value):
    from zcrmsdk.src.com.zoho.crm.api.record import RecordOperations, BodyWrapper
    from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord

    op   = RecordOperations()
    body = BodyWrapper()
    r    = ZCRMRecord()
    r.add_key_value(field_api, value)
    body.set_data([r])
    op.update_record(record_id, module, body)  

#  ─── la funzione richiesta ────────────────────────────────────────────────────
def log(msg: str):
    print(f"{datetime.now().isoformat()} | {msg}")

def load_csv_mapping(csv_path: str) -> dict[str, str]:
    """Legge il CSV solo una volta e restituisce {GID: SharePointPath}."""
    df = pd.read_csv(csv_path)
    return dict(zip(df["Google ID"].astype(str), df["SharePoint Path"]))

# ------------------------------------------------------------------ #
# CORE                                                               #
# ------------------------------------------------------------------ #
def _update_sid_generic(module: str,               # "Deals" o "Accounts"
                        gid_field: str,            # es. "gid" o "gdriveextension__Drive_Folder_ID"
                        sid_field: str,            # di solito "sid"
                        gid: str,
                        new_path: str,
                        dry_run: bool = False):
    """
    Una singola operazione di update:
      1. COQL SELECT id,sid_field WHERE gid_field = gid
      2. se trovato → SetRecordField(module,id,sid_field,new_path)
    """
    query = f'SELECT id, {sid_field} FROM {module} WHERE {gid_field} = "{gid}"'
    try:
        recs = PerformQuery(query) or []
    except Exception as e:
        log(f"⚠️  COQL error ({module}, GID={gid}): {e}")
        return

    if not recs:
        log(f"❌  {module}: nessun record con GID {gid}")
        return

    rec = recs[0]
    rec_id   = rec.get_id()
    old_sid  = rec.get_key_value(sid_field)
    log(f"✅  {module[:-1]} {rec_id}  –  SID attuale: {old_sid!s}  →  nuovo: {new_path}")

    if dry_run:
        log("    (dry-run, skip UPDATE)")
        return

    try:
        SetRecordField(module, rec_id, sid_field, new_path)
        log("    → aggiornato ✔︎")
    except Exception:
        log(f"⚠️  UPDATE fallito:\n{traceback.format_exc()}")

# ------------------------------------------------------------------ #
# WRAPPERS specifici                                                 #
# ------------------------------------------------------------------ #
def update_deal_sid(gid: str, new_path: str, *, dry_run=False):
    _update_sid_generic(
        module="Deals",
        gid_field="gid",
        sid_field="sid",
        gid=gid,
        new_path=new_path,
        dry_run=dry_run
    )

def update_account_sid(gid: str, new_path: str, *, dry_run=False):
    _update_sid_generic(
        module="Accounts",
        gid_field="gdriveextension__Drive_Folder_ID",
        sid_field="sid",
        gid=gid,
        new_path=new_path,
        dry_run=dry_run
    )

# ------------------------------------------------------------------ #
# ORCHESTRATORE                                                      #
# ------------------------------------------------------------------ #
def update_all_sids(csv_path: str, dry_run: bool = True):
    mapping = load_csv_mapping(csv_path)
    log(f"🚀 CSV caricato – {len(mapping)} righe – dry_run={dry_run}")

    # Pass 1: DEALS
    log("▶️  Aggiorno DEALS")
    for gid, path in mapping.items():
        update_deal_sid(gid, path, dry_run=dry_run)
        time.sleep(0.2)          # throttling leggero

    # Pass 2: ACCOUNTS
    log("▶️  Aggiorno ACCOUNTS")
    for gid, path in mapping.items():
        update_account_sid(gid, path, dry_run=dry_run)
        time.sleep(0.2)

    log("🎯  FINITO")
# ---------------------------------------------------------------------------
# MAIN DI TEST
# ---------------------------------------------------------------------------
# if __name__ == "__main__":
#     # TEST_GID = "1RnSI4p7K8N-8WFVGJEwcNP9FWb9V7ayo"   # sostituisci a piacere
#     # info = lookup_deal_by_gid(TEST_GID)

#     # if info:
#     #     print("\n=== RISULTATO ===")
#     #     for k, v in info.items():
#     #         print(f"{k:15}: {v}")
# ── esecuzione ---------------------------------------------------------------
if __name__ == "__main__":
    # df_out = csv_gid_lookup(CSV_PATH)
    # print(df_out.to_string(index=False))
    # df_out.to_csv("gid_sid_lookup.csv", index=False, encoding="utf-8")
    # print("✔︎ Risultati salvati in gid_sid_lookup.csv")
    
    # primi 50
    # for did, gid, sid in sample_deal_gids(10):
    #     print(f"Deal ID: {did} | GID: {gid} | SID: {sid}")
   
    # deals = fetch_deals_with_gid(50)   # oppure 500, 1000, ecc.
    # for deal_id, gid, sid in deals:
    #     print(f"Deal {deal_id} | GID: {gid} | SID: {sid}")
    # print(f"\nTotale trovati: {len(deals)}")

    # result_df = lookup_deals_from_csv("/Users/gfiacconiuv/Desktop/newera/migration_full.csv")

    # # se vuoi visualizzare al volo:
    # print(result_df.head())


    # # --- Esempio di chiamata -----------------------------------------------
    # info = lookup_deal_by_gid("1wZ2v6CAi9TgMIjoJh4D9_Je3fW24iHg4")

    # if info:
    #     print("Deal:", info["deal_id"])
    #     print("  GID:",   info["deal_gid"])
    #     print("  SID:",   info["deal_sid"])
    #     print("  Stage:", info["deal_stage"])

    #     print("Account:", info["account_id"], "-", info["account_name"])
    #     print("  Account GID:", info["account_gid"])
    #     # se nella lookup hai abilitato anche l’account_sid:
    #     # print("  Account SID:", info["account_sid"])
    # else:
    #     print("Nessun record trovato")

    # info = lookup_account_by_gid("1cxrj4YWMThD4KA9kYw0v-5QrXwzOHPzb")
    # if info:
    #     print("\n=== RISULTATO ===")
    #     print("account_id     :", info["account_id"])
    #     print("account_name   :", info["account_name"])
    #     print("account_gid    :", info["account_gid"])
    #     print("account_sid    :", info["account_sid"])
    #     print("-- DEALS collegati --")
    #     for d in info["deals"]:
    #         print(f"  • Deal {d['deal_id']} | gid: {d['deal_gid']} | sid: {d['deal_sid']} | stage: {d['stage']}")

    # acc_info = lookup_account_by_gid("1cxrj4YWMThD4KA9kYw0v-5QrXwzOHPzb")
    # if not acc_info:
    #     print("Account non trovato.")
    # else:
    #     acc_id = acc_info["account_id"]
    #     print("Aggiorno l’Account", acc_id)

    #     # 2. scrivi “prova” nel campo SID
    #     set_account_sid(acc_id, "prova")

    CSV_PATH = "migration_full.csv"
    update_all_sids(CSV_PATH, dry_run=False)

