import sys
import os
from reco_mongodb.mongodbops import MongoDBConnector, InsertOne
from service_logger.service_logger import ServiceLogger
from reco_scrapy.schedule_download_report import DownloadScheduleManager
from reco_scrapy.schedule_status_report import StatusScheduleManager
from reco_scrapy.schedule_request_report import RequestScheduleManager
from reco_schedular.marketplace_schedular import MarketPlaceSchedular
from core.database import PostgreDBConnector
import json
from sqlalchemy import text

class ScrapyScheduleManager:
    def __init__(self, event):
        self.event=event
        """instance
        MongoDB connection object
        """
        try:
            self.db = MongoDBConnector(os.getenv("MONGODB_URL"), os.getenv("MONGODB_SCRAPY_NAME"))
            self.cursor,self.conn = PostgreDBConnector(os.getenv("PG_CLIENT_DATABASE")).get_cursor()
            # self.DOWNLOAD_LAMBDA = DownloadScheduleManager
            # self.STATUS_LAMBDA = StatusScheduleManager
            # self.REQUEST_LAMBDA = RequestScheduleManager

        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("scrapy_Engine").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message)
    def schedule(self):
        ServiceLogger("scrapy_Engine").info(f"In schedule function", '--', "main.py", "status_finder")
        try:
            status = []
            lambda_function = None

            if self.event == TypeMaster.request:
                status = TypeStatusMapping.request
                lambda_function = RequestScheduleManager
                self.scrapy_message_run(status,lambda_function)
                ServiceLogger("scrapy_Engine").info(f"{self.event} Request lambda function is invoked having status {status}", '--', "main.py", "status_finder")

            elif self.event == TypeMaster.status:
                status = TypeStatusMapping.status
                lambda_function = StatusScheduleManager
                self.scrapy_message_run(status, lambda_function)
                ServiceLogger("scrapy_Engine").info(f"{self.event} Status lambda function is invoked having status {status}", '--', "main.py", "status_finder")

            elif self.event == TypeMaster.download:
                status = TypeStatusMapping.download
                lambda_function = DownloadScheduleManager
                self.scrapy_message_run(status, lambda_function)
                ServiceLogger("scrapy_Engine").info(f"{self.event} Download lambda function is invoked having status {status}", '--', "main.py", "status_finder")
            elif self.event == TypeMaster.amazon:
                try:
                    marketplace_id = self.get_mpid("Amazon IN")
                    MarketPlaceSchedular().make_daily_entry_clients(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Amazon Daily Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Amazon Daily Date Entry {str(e)}", '--', "main.py",
                                                        "status_finder")
                return None
            elif self.event == TypeMaster.flipkart:
                try:
                    marketplace_id = self.get_mpid("Flipkart")
                    MarketPlaceSchedular().make_daily_entry_clients(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Flipkart Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Flipkart Daily Date Entry {str(e)}", '--', "main.py",
                                                        "status_finder")
                return None
            elif self.event == TypeMaster.myntra:
                try:
                    marketplace_id = self.get_mpid("Myntra")
                    MarketPlaceSchedular().make_daily_entry_clients(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Myntra Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Myntra Daily Date Entry {str(e)}", '--', "main.py",
                                                         "status_finder")
                return None
            elif self.event == TypeMaster.ajio:
                try:
                    marketplace_id = self.get_mpid("AJIO")
                    MarketPlaceSchedular().make_daily_entry_clients(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Ajio Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Ajio Daily Date Entry {str(e)}", '--', "main.py",
                                                         "status_finder")
                return None
            elif self.event == TypeMaster.amazon_daily:
                try:
                    marketplace_id = self.get_mpid("Amazon IN")
                    MarketPlaceSchedular().daily_fetching(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Amazon Daily Fetching Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Amazon Daily Queue Created {str(e)}", '--', "main.py",
                                                        "status_finder")
                return None
            elif self.event == TypeMaster.flipkart_daily:
                try:
                    marketplace_id = self.get_mpid("Flipkart")
                    MarketPlaceSchedular().daily_fetching(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Flipkart Daily Fetching Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Flipkart Daily Queue Created {str(e)}", '--',
                                                         "main.py",
                                                         "status_finder")
                return None
            elif self.event == TypeMaster.myntra_daily:
                try:
                    marketplace_id = self.get_mpid("Myntra")
                    MarketPlaceSchedular().daily_fetching(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Myntra Daily Fetching Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Myntra Daily Queue Created {str(e)}", '--', "main.py",
                                                        "status_finder")
                return None
            elif self.event == TypeMaster.ajio_daily:
                try:
                    marketplace_id = self.get_mpid("AJIO")
                    MarketPlaceSchedular().daily_fetching(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Ajio Daily Fetching Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Ajio Daily Queue Created {str(e)}", '--', "main.py",
                                                        "status_finder")
                return None
            elif self.event == TypeMaster.flipkart_date_entry:
                try:
                    marketplace_id = self.get_mpid("Flipkart")
                    MarketPlaceSchedular().make_date_entries(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Flipkart  Make Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Flipkart Date entry dropped {str(e)}", '--', "main.py",
                                                        "status_finder")
                return None
            elif self.event == TypeMaster.myntra_date_entry:
                try:
                    marketplace_id = self.get_mpid("Myntra")
                    MarketPlaceSchedular().make_date_entries(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Myntra Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Myntra Date entry dropped {str(e)}", '--',
                                                         "main.py",
                                                         "status_finder")
                return None
            elif self.event == TypeMaster.ajio_date_entry:
                try:
                    marketplace_id = self.get_mpid("AJIO")
                    MarketPlaceSchedular().make_date_entries(marketplace_id)
                    ServiceLogger("scrapy_Engine").info(
                        f"{self.event} Ajio Date Entries Call Dropped", '--', "main.py",
                        "status_finder")
                except Exception as e:
                    ServiceLogger("scrapy_Engine").error(f"Error in Ajio Date entry dropped {str(e)}", '--',
                                                         "main.py",
                                                         "status_finder")
                return None
            else:
                ServiceLogger("scrapy_Engine").info(f"{self.event} lambda function is not available", '--', "main.py", "status_finder")


        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("scrapy_Engine").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message
                                                 )

    def scrapy_message_run(self,status,lambda_function):
        get_accounts = self.get_accounts_by_status(status)
        if get_accounts.alive:
            account_ids = []
            for account in get_accounts:
                create_job = lambda_function(event=account)
                schedule_job = create_job.create_job()
                if schedule_job:
                    ServiceLogger("scrapy_Engine").info(f"Job Was Schedule Successfully", '--', "main.py",
                                                        "status_finder")
                    account_ids.append(account)
                else:
                    ServiceLogger("scrapy_Engine").info(f"Job Was Not Scheduled", '--', "main.py",
                                                        "status_finder")
            self.db.close_connection()
            return {"status_code": 200, "message": f"invoked the lambda function for the Account {account_ids}"}
        else:
            self.db.close_connection()
            ServiceLogger("scrapy_Engine").info("There isn't any Active Account", '--', "main.py", "status_finder")

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
                                                 str(exception_traceback.tb_lineno), exception_message
                                                 )
    def get_mpid(self,marketplace_name):
        try:
            query = f"""SELECT marketplace_id FROM marketplace_master WHERE LOWER (marketplace_name) = '{marketplace_name.lower()}';"""
            print(query)
            self.cursor.execute(query)
            #
            marketplace_result = self.cursor.fetchone()
            if marketplace_result is not None:
                marketplace_id = marketplace_result.get("marketplace_id")
                self.conn.commit()
                self.conn.close()
                print(marketplace_id)
                return int(marketplace_id)
            else:
                raise
        except Exception as e:
            print(e)


class TypeMaster:
    request = "REQUEST"
    status = "STATUS"
    download = "DOWNLOAD"
    amazon = "AMAZON"
    flipkart = "FLIPKART"
    myntra = "MYNTRA"
    e_retail = "E_RETAIL"
    amazon_daily = "AMAZON_DAILY"
    flipkart_daily = "FLIPKART_DAILY"
    myntra_daily = "MYNTRA_DAILY"
    e_Retail_daily = "E_RETAIL_DAILY"
    flipkart_date_entry = "FLIPKART_DATE_ENTRY"
    myntra_date_entry = "MYNTRA_DATE_ENTRY"
    ajio = "AJIO"
    ajio_daily = "AJIO_DAILY"
    ajio_date_entry = "AJIO_DATE_ENTRY"


class TypeStatusMapping:
    request = ["QQ"]
    status = ["RS", "PP"]
    download = ["DD"]
