import sys
import os
from reco_mongodb.mongodbops import MongoDBConnector, InsertOne
from service_logger.service_logger import ServiceLogger
from reco_scrapy.schedule_download_report import DownloadScheduleManager
from reco_scrapy.schedule_status_report import StatusScheduleManager
from reco_scrapy.schedule_request_report import RequestScheduleManager
import json

class ScrapyScheduleManager:
    def __init__(self, event):
        self.event=event
        """instance
        MongoDB connection object
        """
        try:
            self.db = MongoDBConnector(os.getenv("MONGODB_URL"), os.getenv("MONGODB_SCRAPY_NAME"))
            self.DOWNLOAD_LAMBDA = DownloadScheduleManager
            self.STATUS_LAMBDA = StatusScheduleManager
            self.REQUEST_LAMBDA = RequestScheduleManager

        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("scrapy_Engine").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message,
                                                 job_id=self.event.get("_id"),
                                                 client_id=self.event.get("cid"),
                                                 marketplace_id=self.event.get("mpid"),
                                                 channel_id=self.event.get("mpid"),
                                                 account_id=self.event.get("acid")
                                                 )
    def schedule(self):
        ServiceLogger("scrapy_Engine").info(f"In schedule function", '--', "main.py", "status_finder")

        try:
            status = []
            lambda_function = None

            if self.event == TypeMaster.request:
                status = TypeStatusMapping.request
                lambda_function = self.REQUEST_LAMBDA
                ServiceLogger("scrapy_Engine").info(f"{self.event} Request lambda function is invoked having status {status}", '--', "main.py", "status_finder")

            elif self.event == TypeMaster.status:
                status = TypeStatusMapping.status
                lambda_function = self.STATUS_LAMBDA
                ServiceLogger("scrapy_Engine").info(f"{self.event} Status lambda function is invoked having status {status}", '--', "main.py", "status_finder")

            elif self.event == TypeMaster.download:
                status = TypeStatusMapping.download
                lambda_function = self.DOWNLOAD_LAMBDA
                ServiceLogger("scrapy_Engine").info(f"{self.event} Download lambda function is invoked having status {status}", '--', "main.py", "status_finder")

            else:
                ServiceLogger("scrapy_Engine").info(f"{self.event} lambda function is not available", '--', "main.py", "status_finder")
            get_accounts = self.get_accounts_by_status(status)
            if get_accounts.alive:
                account_ids = []
                for account in get_accounts:
                    create_job = lambda_function(event=account)
                    schedule_job = create_job.create_job()
                    if schedule_job:
                        ServiceLogger("scrapy_Engine").info(f"Job Was Schedule Successfully", '--', "main.py", "status_finder")
                        account_ids.append(account)
                    else:
                        ServiceLogger("scrapy_Engine").info(f"Job Was Not Scheduled", '--', "main.py",
                                                            "status_finder")
                self.db.close_connection()
                return {"status_code": 200,"message":f"invoked the lambda function for the Account {account_ids}"}
            else:
                self.db.close_connection()
                ServiceLogger("scrapy_Engine").info("There isn't any Active Account", '--', "main.py", "status_finder")

        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("scrapy_Engine").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message,
                                                 filename="schedule_manager"
                                                 )

    def get_accounts_by_status(self, status):
        ServiceLogger("scrapy_Engine").info(f"Get Accounts by status function", '--', "main.py", "status_finder")
        try:
            from datetime import timedelta, date
            today = str(date.today() - timedelta(days=0))
            toprocess = 1
            if "QQ" in status:
                toprocess = 0

            pipeline = [
                {
                    "$match": {
                        "status": {
                            "$in": status,
                        },
                        "toprocess":toprocess
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "cid": "$cid",
                            "mpid": "$mpid",
                            "acid": "$acid",
                            "vendor":"$vendor"
                        }
                    }
                },
                {
                    "$project": {
                        "cid": "$_id.cid",
                        "mpid": "$_id.mpid",
                        "acid": "$_id.acid",
                        "vendor":"$_id.vendor",
                        "_id": 0.0
                    }
                }
            ]
            get_accounts = self.db.aggregate_query(
                collection_name="master_messages",
                pipeline=pipeline
            )
            # print(list(get_accounts))
            self.db.close_connection()
            return get_accounts
        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("scrapy_Engine").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message,
                                                 filename="schedule_manager"
                                                 )

class TypeMaster:
    request = "REQUEST"
    status = "STATUS"
    download = "DOWNLOAD"


class TypeStatusMapping:
    request = ["QQ"]
    status = ["RS", "PP"]
    download = ["DD"]
