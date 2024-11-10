from dotenv import load_dotenv
load_dotenv()
import base64
import json
import sys
import os
from reco_mongodb.mongodbops import MongoDBConnector
from service_logger.service_logger import ServiceLogger
from bson import ObjectId
import redis

from datetime import datetime

"""
use this class to schedule Download Reports
"""


class DownloadScheduleManager:

    def __init__(self, event):
        try:
            self.event = event
            self.db = MongoDBConnector(os.getenv('MONGODB_URL'), os.getenv('MONGODB_SCRAPY_NAME'))
            self.report_config = "vinreco_Master.json"
        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("reco_schedular_scrapy").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message,
                                                 job_id=self.event.get("_id"),
                                                 client_id=self.event.get("cid"),
                                                 marketplace_id=self.event.get("mpid"),
                                                 channel_id=self.event.get("mpid"),
                                                 account_id=self.event.get("acid")
                                                , stage="create_download_queue"
                                                 )

    '''
    this function will create download job for report for each account
    '''

    def create_job(self):
        from tasks import scrapy_worker
        ServiceLogger("scrapy_Engine").info("In create_download_job Function: ", '--', "main.py", "create_download_job",stage="create_download_queue")

        try:
            ServiceLogger("scrapy_Engine").info("get_report_list_with_status is going to execute", '--', "main.py",
                                                "create_download_job")
            get_report_list1 = self.get_report_list_with_status(self.event)
            report_conf = self.report_configs()
            if get_report_list1.alive:
                report_details_id_list = []
                for report_details in get_report_list1:
                    queue_name = self.find_queue(report_details.get('mpid'),report_details.get("report_type"))
                    report_details.update({"_id": str(report_details.get('_id'))})
                    object_id = report_details.get('_id')
                    job_config = list(
                        filter(lambda record: record['report_type'] == report_details['report_type'], report_conf))
                    concurrency_flag = job_config[0].get('concurrency')
                    self.delay = job_config[0].get('delay_minuite')
                    report_details.update({"action": "Download"})
                    running_status = self.report_running_status(report_details)
                    # check_delay = self.check_delay(report_details)
                    if report_details.get('report_type') == 'login' or self.get_login_validation(self.event):
                        message = json.dumps(report_details)
                        # print(message)
                        message_bytes = message.encode('ascii')
                        base64_bytes = base64.b64encode(message_bytes)
                        base64_message = base64_bytes.decode('ascii')
                        ServiceLogger("scrapy_Engine").info(f'This report {report_details} send for download', '--',
                                                            "main.py",
                                                            "create_download_job",stage="create_download_queue")

                        scrapy_worker.apply_async(args=[base64_message], queue=queue_name)

                        report_details_id_list.append(report_details.get('_id'))
                        self.db.update_one("master_messages", {'_id': ObjectId(object_id)}, {"$set": {'toprocess': 2}})
                    else:
                        ServiceLogger("scrapy_Engine").info(
                            f'This report {report_details} already in progress', '--',
                            "main.py", "create_download_job",stage="create_download_queue")
                self.db.close_connection()
                return {"status_code": 200, "message": f"Successfully Updated object id is : {report_details_id_list}"}
            else:
                self.db.close_connection()
                ServiceLogger("scrapy_Engine").info(
                    f'This account id {self.event.get("account_id")} has no Report with DD status', '--', "main.py",
                    "create_download_job",stage="create_download_queue")
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
                                                 , stage="create_download_queue"
                                                 )

    def find_queue(self, mpid,report_type):
        # STAGE = os.getenv("STAGE")
        # server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
        # redis_client = redis.from_url(server_url, decode_responses=True)
        # message = f"{STAGE}_{int(mpid)}_queue_scrapy"
        # if report_type == "FSN_Reports":
        #     message = f"{STAGE}_{int(mpid)}_queue_scrapy_commission"
        # try:
        #     queue_name = redis_client.get(message)
        # except:
        #     queue_name = {}
        # # print(queue_name)
        return "reco_scrapy"

    def report_configs(self):
        with open(self.report_config, "r") as ymlfile:
            data = json.loads(ymlfile.read())
        return data

    def check_delay(self, report):
        import datetime
        ServiceLogger("scrapy_Engine").info(f"In check_delay fun", '--', "main.py", "status_finder",stage="create_download_queue")

        try:
            delay = self.delay
            ServiceLogger("scrapy_Engine").info(f"executing get_last_request_time fun", '--', "main.py",
                                                "status_finder",stage="create_download_queue")

            last_request_time = self.get_last_request_time1(report)
            last_request_comman = self.get_last_request_time2(report)
            if last_request_time and last_request_comman:
                if last_request_time == '':
                    last_request_time = datetime.datetime.today()
                # print('delay---->',delay)
                delay = datetime.timedelta(minutes=delay)
                delay_comman = datetime.timedelta(minutes=2)
                to_make_request = last_request_time + delay
                to_make_request_comman = last_request_time + delay_comman
                current_datetime = datetime.datetime.today()
                if current_datetime > to_make_request and current_datetime > to_make_request_comman:
                    # print(report)
                    return True
                else:
                    print(report)
                    ServiceLogger("scrapy_Engine").info(
                        f"you have to wait few minuites, please try again later\n\n{self.event}", '--', "main.py",
                        "status_finder",stage="create_download_queue")

                    return False
            else:
                return True
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
                                                 ,stage="create_download_queue")

    def get_last_request_time1(self, report_details):
        try:
            ServiceLogger("scrapy_Engine").info(f"In get_last_request_time fun:-", '--', "main.py", "status_finder",stage="create_download_queue")
            server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            r = redis.from_url(server_url, decode_responses=True)
            message = f"{report_details.get('cid')}_{report_details.get('mpid')}_{report_details.get('acid')}_{report_details.get('vendor')}_{report_details.get('report_type')}"
            try:
                value = json.loads(r.get(message))
            except:
                value = {}
            last_request_time = value.get('request_time')
            try:
                datetime_object = datetime.strptime(last_request_time, '%Y-%m-%d %H:%M:%S.%f')
            except:
                datetime_object = ''
            return datetime_object
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
                                                 , stage="create_download_queue"
                                                 )

    def get_last_request_time2(self, report_details):
        try:
            ServiceLogger("scrapy_Engine").info(f"In get_last_request_time fun:-", '--', "main.py", "status_finder")
            server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            r = redis.from_url(server_url, decode_responses=True)
            message = f"{report_details.get('cid')}_{report_details.get('mpid')}_{report_details.get('acid')}"
            try:
                value = json.loads(r.get(message))
            except:
                value = {}
            last_request_time = value.get('request_time')
            try:
                datetime_object = datetime.strptime(last_request_time, '%Y-%m-%d %H:%M:%S.%f')
            except:
                datetime_object = ''
            return datetime_object
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

    def report_running_status(self, report_details):
        ServiceLogger("scrapy_Engine").info("In report_running_status Function Function: ", '--', "main.py",
                                            "report_running_status")

        try:
            server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            r = redis.from_url(server_url, decode_responses=True)
            message = f"{report_details.get('cid')}_{report_details.get('mpid')}_{report_details.get('acid')}_{report_details.get('vendor')}_{report_details.get('report_type')}"
            try:
                value = json.loads(r.get(message))
            except:
                value = {}
            running_status = value.get('running')
            return running_status
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

    def get_report_list_with_status(self, each_report):
        ServiceLogger("scrapy_Engine").info("In get_report_list_with_status Function: ", '--', "main.py",
                                            "get_report_list_with_status")

        try:
            from datetime import timedelta, date
            today = str(date.today() - timedelta(days=0))
            pipeline = [
                {
                    "$match": {
                        "status": {"$in": ["DD"]},
                        "cid": each_report.get("cid"),
                        "mpid": each_report.get("mpid"),
                        "acid": each_report.get("acid"),
                        "toprocess": 1
                    }
                },
                {
                    "$sort": {
                        "priority": 1
                    }
                },
                {'$limit': 1}
            ]
            get_report = self.db.aggregate_query(
                collection_name="master_messages",
                pipeline=pipeline
            )
            # print(list(get_report))
            return get_report
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

    def get_login_validation(self, each_report):
        ServiceLogger("scrapy_Engine").info("In get_report_list_with_status Function: ", '--', "main.py",
                                            "get_report_list_with_status")

        try:
            pipeline = [
                {
                    "$match": {
                        "cid": each_report.get("cid"),
                        "mpid": each_report.get("mpid"),
                        "acid": each_report.get("acid"),
                        "vendor": each_report.get("vendor")
                    }
                },
                {"$project": {"login": 1}},
            ]
            get_report = self.db.aggregate_query(
                collection_name="login_details",
                pipeline=pipeline
            )
            # print(list(get_report))
            status = list(get_report)[0].get('login')
            if status == "valid":
                return True
            else:
                return False
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