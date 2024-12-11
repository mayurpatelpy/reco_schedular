from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2
import psycopg2.extras
import sys
from service_logger.service_logger import ServiceLogger



class PostgreDBConnector:
    def __init__(self,db_name):
        try:
            self.host_server = os.environ.get('PG_HOST')
            self.db_server_port = os.environ.get('PG_PORT')
            self.database_name = db_name
            self.db_user = os.environ.get('PG_USERNAME')
            self.db_password = os.environ.get('PG_PASSWORD')

        except Exception as e:
            exception_message = str(e)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("amazon_sp_api").error(exception_message, class_name, filename, '__init__',
                                                 str(exception_traceback.tb_lineno), exception_message)
            raise e

    def get_cursor(self):
        try:
            connection = psycopg2.connect(database=self.database_name, user=self.db_user, password=self.db_password,
                                          host=self.host_server, port=self.db_server_port)
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            return cursor,connection
        except Exception as e:
            exception_message = str(e)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("amazon_sp_api").error(exception_message, class_name, filename, 'get_cursor',
                                                 str(exception_traceback.tb_lineno), exception_message)
            raise e