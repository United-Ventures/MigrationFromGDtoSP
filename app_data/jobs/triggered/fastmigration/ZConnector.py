from zcrmsdk.src.com.zoho.crm.api.user_signature import UserSignature
from zcrmsdk.src.com.zoho.crm.api.dc import EUDataCenter
from zcrmsdk.src.com.zoho.api.authenticator.store import DBStore, FileStore
from zcrmsdk.src.com.zoho.api.logger import Logger
from zcrmsdk.src.com.zoho.crm.api.initializer import Initializer
from zcrmsdk.src.com.zoho.api.authenticator.oauth_token import OAuthToken, TokenType
from zcrmsdk.src.com.zoho.crm.api.sdk_config import SDKConfig
import pandas as pd
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote

#Scopes ZohoCRM.users.ALL,ZohoCRM.org.ALL,ZohoCRM.settings.ALL,ZohoCRM.modules.ALL,ZohoCRM.bulk.ALL, ZohoCRM.bulk.READ, ZohoCRM.bulk.CREATE, ZohoCRM.coql.READ
class SDKInitializer(object):
    @staticmethod
    def initialize():
        """
        Create an instance of Logger Class that takes two parameters
        1 -> Level of the log messages to be logged. Can be configured by typing Logger.Levels "." and choose any level from the list displayed.
        2 -> Absolute file path, where messages need to be logged.
        """
        logger = Logger.get_instance(level=Logger.Levels.INFO,
                                     file_path='zohoLog.log')

        # Create an UserSignature instance that takes user Email as parameter
        user = UserSignature(email='gfiacconi@unitedventures.com')

        """
        Configure the environment
        which is of the pattern Domain.Environment
        Available Domains: USDataCenter, EUDataCenter, INDataCenter, CNDataCenter, AUDataCenter
        Available Environments: PRODUCTION(), DEVELOPER(), SANDBOX()
        """
        # environment = EUDataCenter.SANDBOX()

        # """
        # Create a Token instance that takes the following parameters
        # 1 -> OAuth client id.
        # 2 -> OAuth client secret.
        # 3 -> Grant token.
        # 4 -> Refresh token.
        # 5 ->> OAuth redirect URL.
        # 6 ->> id
        # """
        # token = OAuthToken(client_id='1000.FJQ9PFGDIELH4MUBZZ4S8MJXWQG5CI',
        #                    client_secret='9cc7e2978294e50948899b950ae0eb4d17d8c08fcb',
        #                    token="1000.5096312d9d99c70d3783e8117c7780d4.2bde035313d28fcdccac6c75e4798600",
        #                    token_type=TokenType.GRANT,
        #                    redirect_url='abs.com')
        

        environment = EUDataCenter.PRODUCTION()

        """
        Create a Token instance that takes the following parameters
        1 -> OAuth client id.
        2 -> OAuth client secret.
        3 -> Grant token.
        4 -> Refresh token.
        5 ->> OAuth redirect URL.
        6 ->> id
        """
        token = OAuthToken(client_id='1000.FJQ9PFGDIELH4MUBZZ4S8MJXWQG5CI',
                           client_secret='9cc7e2978294e50948899b950ae0eb4d17d8c08fcb',
                           token="1000.0fb70552f828170db47aa35c022484ec.7cf1528f8622e3386bbcb546c8908dac",
                           token_type=TokenType.GRANT,
                           redirect_url='abs.com')




        """
        Create an instance of TokenStore
        1 -> Absolute file path of the file to persist tokens
        """
        print(token)
        store = FileStore(file_path='python_sdk_tokens.txt')
        #store = DBStore(host='127.0.0.1', database_name='unitedventures', user_name='root', password='password', port_number='3306')

        """
        auto_refresh_fields (Default value is False)
            if True - all the modules' fields will be auto-refreshed in the background, every hour.
            if False - the fields will not be auto-refreshed in the background. The user can manually delete the file(s) or refresh the fields using methods from ModuleFieldsHandler(zcrmsdk/src/com/zoho/crm/api/util/module_fields_handler.py)

        pick_list_validation (Default value is True)
        A boolean field that validates user input for a pick list field and allows or disallows the addition of a new value to the list.
            if True - the SDK validates the input. If the value does not exist in the pick list, the SDK throws an error.
            if False - the SDK does not validate the input and makes the API request with the user’s input to the pick list

        connect_timeout (Default value is None) 
            A  Float field to set connect timeout

        read_timeout (Default value is None) 
            A  Float field to set read timeout
        """
        config = SDKConfig(auto_refresh_fields=True, pick_list_validation=False)

        """
        The path containing the absolute directory path (in the key resource_path) to store user-specific files containing information about fields in modules. 
        """
        resource_path = "./resources"

        """
        Call the static initialize method of Initializer class that takes the following arguments
        1 -> UserSignature instance
        2 -> Environment instance
        3 -> Token instance
        4 -> TokenStore instance        
        5 -> SDKConfig instance
        6 -> resource_path
        7 -> Logger instance. Default value is None
        8 -> RequestProxy instance. Default value is None
        """
        Initializer.initialize(user=user,
                               environment=environment,
                               token=token,
                               store=store,
                               sdk_config=config,
                               resource_path=resource_path,
                               logger=logger)
SDKInitializer.initialize()


