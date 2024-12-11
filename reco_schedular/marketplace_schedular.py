from dotenv import load_dotenv
from dulwich.porcelain import status

load_dotenv()

import json
from service_logger.service_logger import ServiceLogger
import os, sys
from reco_mongodb.mongodbops import MongoDBConnector, InsertOne
from datetime import datetime
from bson import ObjectId
from core.database import PostgreDBConnector
from utils.dependency import call_internal_services


class MarketPlaceSchedular:
    def __init__(self):
        # self.request = request
        self.mongocurorDB = MongoDBConnector(os.getenv('MONGODB_URL'), os.getenv("MONGODB_NAME"))
        self.cursor,self.conn = PostgreDBConnector(os.getenv("PG_CLIENT_DATABASE")).get_cursor()

    # def make_request(self):
    #     try:
    #         if self.request.get('type') == "AMAZON":#31 18 * * ? *
    #             result = self.make_daily_entry_clients(mpid=17)
    #
    #         if self.request.get('type') == "FLIPKART":# 31 18 1-31/2 * ? *
    #             result = self.make_daily_entry_clients(mpid=10)
    #
    #         if self.request.get('type') == "MYNTRA":#31 18 2-31/2 * ? *
    #             result = self.make_daily_entry_clients(mpid=91)
    #
    #         if self.request.get('type')=="E_RETAIL":#0 0 */3 * ? *
    #             result = self.make_daily_entry_clients(mpid=501,domain="OMS")
    #
    #         if self.request.get('type') == "AMAZON_DAILY": #5 min
    #             result = self.daily_fetching(mpid=17)
    #
    #         if self.request.get('type') == "FLIPKART_DAILY": #1 min
    #             result = self.daily_fetching(mpid=10)
    #
    #         if self.request.get('type') == "MYNTRA_DAILY": #1 min
    #             result = self.daily_fetching(mpid=91)
    #
    #         if self.request.get('type') == "E_RETAIL_DAILY": #1 min
    #             result=self.daily_fetching(mpid=501)
    #
    #         if self.request.get('type') == "FLIPKART_DATE_ENTRY":#10 min
    #             result = self.make_date_entries(mpid=10)
    #
    #         if self.request.get('type') == "MYNTRA_DATE_ENTRY": #10 min
    #             result = self.make_date_entries(mpid=91)
    #     except Exception as e:
    #         exception_message = str(e)
    #         exception_type, _, exception_traceback = sys.exc_info()
    #         filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
    #         # Logger('vinreco-marketplace-schedular').error(
    #         #     f"{exception_message} {exception_type} {filename}, Line {exception_traceback.tb_lineno}")
    #         raise e

    def get_active_clients(self, mpid):
        try:
            pipeline = [
                {
                    "$match": {
                        "mpid": mpid,
                        "endt": datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d'),
                        "status": 0
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "acid": 1,
                        "mpid": 1,
                        "cid": 1
                    }
                },
                {
                    "$limit": 1
                }
            ]
            result = list(self.mongocurorDB.aggregate_query("daily_pulling_logs", pipeline))
            return result
        except Exception as e:
            exception_message = str(e)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("vinreco-marketplace-schedular").error(exception_message, class_name, filename,
                                                                 'get_active_clients',
                                                                 str(exception_traceback.tb_lineno),
                                                                 exception_message, )
            raise e

    def make_daily_entry_clients(self, mpid,domain=None):
        try:
            query=''
            if not domain:
                query = f"""select mc.marketplace_id as mpid, mc.client_id as cid, mc.id as acid from marketplace_connectors as mc
                                    join clients as c
                                    on mc.client_id = c.id and mc.is_active =true and mc.admin_active=true and mc.marketplace_id={mpid}
                                    where mc.client_id not in (718,1)
                        """
            if domain=="OMS":
                query = f"""select mc.marketplace_id as mpid, mc.client_id as cid, mc.id as acid from oms_connectors as mc
                                                    join clients as c
                                                    on mc.client_id = c.id and mc.is_active =1 and mc.admin_active=1 and mc.marketplace_id={mpid}"""
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            self.conn.close()
            batch_insert = []
            if result is not None:
                for record in result:
                    insert_dict = {
                        "cid": record.get('cid'),
                        "acid": record.get('acid'),
                        "mpid": record.get('mpid'),
                        'endt': datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d'),
                        'endttm': datetime.now(),
                        'status': 0
                    }
                    print(insert_dict)
                    batch_insert.append(InsertOne(insert_dict))
            self.mongocurorDB.bulk_write("daily_pulling_logs", batch_insert)
            return {"statusCode": 200, "message": "Records inserted successfully in reco_daily_log"}
        except Exception as e:
            exception_message = str(e)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("vinreco-marketplace-schedular").error(exception_message, class_name, filename,
                                                                 'get_active_clients',
                                                                 str(exception_traceback.tb_lineno),
                                                                 exception_message, )
            raise e

    def daily_fetching(self, mpid):
        result = self.get_active_clients(mpid)
        if len(result):
            record = result[0]
            # client = boto3.client("lambda")
            ServiceLogger("vinreco-marketplace-schedular").info(
                f"Active client with client_id:{record['cid']}, account_id:{record['acid']}, marketplace_id:{record['mpid']} process started")
            match record.get('mpid'):
                case 100:
                    result = list(self.mongocurorDB.find_query('reportslogtracking',
                                                               {"acid": record.get('acid'), "cid": record.get('cid'),
                                                                "repst": "QQ", "reptp": {
                                                                   "$nin": ["MTR_B2C", "MTR_B2B", "AFNINVENTORY",
                                                                            "FLEX_RETURNS"]}}))
                    if len(result):
                        self.mongocurorDB.update_one("daily_pulling_logs",
                                                     {"_id": ObjectId(record.get('_id'))},
                                                     {"$set": {"status": 4, "date_entry_updt": datetime.now()}})
                    else:
                        request = {
                            "client_id": record.get('cid'),
                            "account_id": record.get('acid'),
                            "marketplace_id": record.get('mpid')
                        }
                        ServiceLogger("vinreco-marketplace-schedular").info(
                            f"Invoking Date-Entries having request-{request}")
                        # response = client.invoke_async(
                        #     FunctionName=LambdaFunctionName.date_entry_manager,
                        #     InvokeArgs=json.dumps(request).encode()
                        # )
                        self.mongocurorDB.update_one("daily_pulling_logs",
                                                     {"_id": ObjectId(record.get('_id'))},
                                                     {"$set": {"status": 3, "date_entry_updt": datetime.now()}})
                case 200:
                    request = {
                        "client_id": record.get('cid'),
                        "account_id": record.get('acid'),
                        "marketplace_id": record.get('mpid'),
                        "call_type": "Login",
                        "report_type": "login"
                    }
                    ServiceLogger("vinreco-marketplace-schedular").info(
                        f"Invoking Flipkart-Schedular having request-{request}")
                    # response = client.invoke_async(
                    #     FunctionName=LambdaFunctionName.flipkart_schedular,
                    #     InvokeArgs=json.dumps(request).encode()
                    # )
                    response = call_internal_services(method="POST", endpoint="/v1/schedular/",
                                                            body=request)
                    if response.status_code == 200:
                        ServiceLogger("scrapy_Engine").info(f"Schedular Call Dropped Successfully", '--',
                                                             "marketplace_schedular.py", "schedular")
                        self.mongocurorDB.update_one("daily_pulling_logs",
                                                     {"_id":ObjectId(record.get('_id'))},
                                                     {"$set": {"status": 1,"lstupdttm":datetime.now()}})
                    else:
                        ServiceLogger("scrapy_Engine").error(
                            f"Getting Error On Internal Service API {response.status_code}", '--',
                            "marketplace_schedular.py", "schedular")
                case 300:
                    request = {
                        "client_id": record.get('cid'),
                        "account_id": record.get('acid'),
                        "marketplace_id": record.get('mpid'),
                        "call_type": "Login",
                        "report_type": "login"
                    }
                    ServiceLogger("vinreco-marketplace-schedular").info(
                        f"Invoking myntra-schedular-{request}")
                    # response = client.invoke_async(
                    #     FunctionName=LambdaFunctionName.myntra_schedular,
                    #     InvokeArgs=json.dumps(request).encode()
                    # )
                    response = call_internal_services(method="POST", endpoint="/v1/schedular/",
                                                      body=request)
                    if response.status_code == 200:
                        ServiceLogger("scrapy_Engine").info(f"Schedular Call Dropped Successfully", '--',
                                                             "marketplace_schedular.py", "schedular")
                        self.mongocurorDB.update_one("daily_pulling_logs",
                                                     {"_id":ObjectId(record.get('_id'))},
                                                     {"$set": {"status": 1,"lstupdttm":datetime.now()}})
                    else:
                        ServiceLogger("scrapy_Engine").error(f"Getting Error On Internal Service API {response.status_code}", '--', "marketplace_schedular.py", "schedular")
                case 131:
                    request = {
                        "client_id": record.get('cid'),
                        "account_id": record.get('acid'),
                        "marketplace_id": record.get('mpid'),
                        "call_type": "Login",
                        "report_type": "login"
                    }
                    ServiceLogger("vinreco-marketplace-schedular").info(
                        f"Invoking Tatacliq-Schedular having request-{request}")
                    # response = client.invoke_async(
                    #     FunctionName=LambdaFunctionName.tatacliq_schedular,
                    #     InvokeArgs=json.dumps(request).encode()
                    # )
                    response = call_internal_services(method="POST", endpoint="/v1/schedular/",
                                                      body=request)
                    call_internal_services(method="POST", endpoint="/v1/marketplace/connector", body=request)
                    self.mongocurorDB.update_one("daily_pulling_logs",
                                                 {"_id":ObjectId(record.get('_id'))},
                                                 {"$set": {"status": 1,"lstupdttm":datetime.now()}})
                case 501:
                    request = {
                        "client_id": record.get('cid'),
                        "account_id": record.get('acid'),
                        "marketplace_id": record.get('mpid')
                    }
                    ServiceLogger("vinreco-marketplace-schedular").info(
                        f"Invoking Date-Entries having request-{request}")
                    # response = client.invoke_async(
                    #     FunctionName=LambdaFunctionName.date_entry_manager,
                    #     InvokeArgs=json.dumps(request).encode()
                    # )
                    self.mongocurorDB.update_one("daily_pulling_logs",
                                                 {"_id": ObjectId(record.get('_id'))},
                                                 {"$set": {"status": 3, "date_entry_updt": datetime.now()}})
        else:
            ServiceLogger("vinreco-marketplace-schedular").info("There are no active clients")


    def make_date_entries(self,mpid):
        result=list(self.mongocurorDB.find_query("daily_pulling_logs",{"mpid":mpid,"status":0}))
        if len(result):
            ServiceLogger("vinreco-marketplace-schedular").info("Account left to be processed")
        else:
            make_date_entries=list(self.mongocurorDB.aggregate_query("daily_pulling_logs",[
                {
                    "$match":{
                        "mpid":mpid
                    }
                },
                {
                    "$sort":{
                        "lstupdttm":-1
                    }
                },
                {
                    "$addFields":{
                        "minutediff":{
                            "$dateDiff":{
                                "startDate": "$lstupdttm",
                                "endDate": datetime.now(),
                                "unit": "minute"
                            }
                        }
                    }
                },
                {
                    "$match":{
                        "minutediff":{
                            "$gt":20
                        }
                    }
                },
                {
                  "$match":{
                      "status":2
                  }
                },
                {
                    "$limit":1
                }
            ]))
            if len(make_date_entries):
                record=make_date_entries[0]
                if record.get('mpid')==200:
                    query=f"""
                            SELECT * FROM marketplace_connectors as mc where mc.marketplace_id={record.get('mpid')} and mc.client_id={record.get('cid')} and mc.id={record.get('acid')}     
                        """
                    self.cursor.execute(query)
                    get_token = [dict(row) for row in self.cursor.fetchall()][0]
                    if get_token.get('valid_token')==False:
                        self.mongocurorDB.update_one("daily_pulling_logs",
                                                     {"_id": ObjectId(record.get('_id'))},
                                                     {"$set": {"status": 4, "date_entry_updt": datetime.now()}})
                    else:
                        # client = boto3.client("lambda")
                        request = {
                            "client_id": record.get('cid'),
                            "account_id": record.get('acid'),
                            "marketplace_id": record.get('mpid')
                        }
                        ServiceLogger("vinreco-marketplace-schedular").info(
                            f"Invoking Date-Entries having request-{request}")
                        # response = client.invoke_async(
                        #     FunctionName=LambdaFunctionName.date_entry_manager,
                        #     InvokeArgs=json.dumps(request).encode()
                        # )
                        response = call_internal_services(method="POST", endpoint="/v1/dateentry/",
                                                          body=request)
                        if response.status_code == 200:
                            ServiceLogger("scrapy_Engine").info(f"Date Entry Call Dropped Successfully", '--',
                                                                 "marketplace_schedular.py", "schedular")
                            self.mongocurorDB.update_one("daily_pulling_logs",
                                                            {"_id":ObjectId(record.get('_id'))},
                                                         {"$set": {"status": 3,"date_entry_updt":datetime.now()}})
                        else:
                            ServiceLogger("scrapy_Engine").error(f"Error Date Entry Call Dropped {response.status_code}", '--',
                                                                "marketplace_schedular.py", "schedular")
                    self.conn.close()

                else:
                    # client = boto3.client("lambda")
                    request = {
                        "client_id": record.get('cid'),
                        "account_id": record.get('acid'),
                        "marketplace_id": record.get('mpid')
                    }
                    ServiceLogger("vinreco-marketplace-schedular").info(
                        f"Invoking Date-Entries having request-{request}")
                    # response = client.invoke_async(
                    #     FunctionName=LambdaFunctionName.date_entry_manager,
                    #     InvokeArgs=json.dumps(request).encode()
                    # )
                    response = call_internal_services(method="POST", endpoint="/v1/dateentry/",
                                                      body=request)
                    if response.status_code == 200:
                        ServiceLogger("scrapy_Engine").info(f"Date Entry Call Dropped Successfully", '--',
                                                            "marketplace_schedular.py", "schedular")
                        self.mongocurorDB.update_one("daily_pulling_logs",
                                                     {"_id": ObjectId(record.get('_id'))},
                                                     {"$set": {"status": 3, "date_entry_updt": datetime.now()}})
                    else:
                        ServiceLogger("scrapy_Engine").error(f"Error Date Entry Call Dropped {response.status_code}",
                                                             '--',
                                                             "marketplace_schedular.py", "schedular")