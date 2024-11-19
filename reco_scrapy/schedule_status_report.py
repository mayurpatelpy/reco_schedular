from dotenv import load_dotenv
load_dotenv()
import base64
import json
import sys
import os
import redis

from service_logger.service_logger import ServiceLogger
from reco_mongodb.mongodbops import MongoDBConnector


class StatusScheduleManager:

    def __init__(self, event):
        try:
            self.event = event
            ServiceLogger("schedule-status-job").info("In create_status_job Function: ", str(self.__class__.__name__),
                                                      'main.py', "StatusScheduleManager",
                                                      job_id=self.event.get("_id"),
                                                      client_id=self.event.get("cid"),
                                                      marketplace_id=self.event.get("mpid"),
                                                      channel_id=self.event.get("mpid"),
                                                      account_id=self.event.get("acid")
                                                      , stage="create_status_queue")
            # if os.getenv("STAGE") == "prod":
            #     self.report_config = os.getenv("REPORT_CONFIG")
            # else:
            self.report_config = "vinreco_status_job.json"

            self.db = MongoDBConnector(os.getenv("MONGODB_URL"), os.getenv("MONGODB_SCRAPY_NAME"))
        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("schedule-status-job").error(exception_message, class_name, filename, filename,
                                                 str(exception_traceback.tb_lineno), exception_message,
                                                       job_id=self.event.get("_id"),
                                                       client_id=self.event.get("cid"),
                                                       marketplace_id=self.event.get("mpid"),
                                                       channel_id=self.event.get("mpid"),
                                                       account_id=self.event.get("acid")
                                                       , stage="create_status_queue")

    def create_job(self):
        from tasks import scrapy_worker
        ServiceLogger("schedule-status-job").info("In create_status_job Function: ", str(self.__class__.__name__),
                                                  'main.py', "create_status_job",
                                                  job_id=self.event.get("_id"),
                                                  client_id=self.event.get("cid"),
                                                  marketplace_id=self.event.get("mpid"),
                                                  channel_id=self.event.get("mpid"),
                                                  account_id=self.event.get("acid")
                                                  , stage="create_status_queue")
        try:
            get_report_list1 = self.get_report_by_status(["RS", "PP"],self.event)
            if get_report_list1.alive:
                report_details_id_list=[]
                for report_details in get_report_list1:
                    queue_name = self.find_queue(report_details.get('mpid'))
                    running_status = self.report_running_status(report_details)
                    report_details.update({"_id": str(report_details.get('_id'))})
                    report_details.update({"action": "Status"})
                    if running_status != 1:
                        report_conf = self.report_configs()
                        rtype = report_details['report_type'].lower()
                        msg_config = list(filter(lambda record: record['report_type'].lower() == rtype, report_conf))
                        if msg_config:
                            msg_spider = msg_config[0].get('spider')
                            spider = {'spider_name': msg_spider}
                            report_details.update(spider)
                            message = json.dumps(report_details)
                            message_bytes = message.encode('ascii')
                            base64_bytes = base64.b64encode(message_bytes)
                            base64_message = base64_bytes.decode('ascii')
                            ServiceLogger("schedule-status-job").info(f'Worker to be executed for {report_details}')
                            scrapy_worker.apply_async(args=[base64_message], queue=queue_name)
                            report_details_id_list.append(report_details.get('_id'))
                        else:
                            ServiceLogger("schedule-status-job").info(f'No Report Type Found for account id {self.event.get("acid")} , channel id {self.event.get("mpid")}', stage="create_status_queue")
                self.db.close_connection()
                return {'status_code':200,'message':f'successfully updated object_id is:{report_details_id_list}'}
            else:
                self.db.close_connection()
                ServiceLogger("schedule-status-job").info(
                    f'This account id {self.event.get("acid")},channel id {self.event.get("mpid")}has no Report with RS,PP status', stage="create_status_queue")

        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("schedule-status-job").error(exception_message, class_name, filename, filename,
                                                         str(exception_traceback.tb_lineno), exception_message,
                                                       job_id=self.event.get("_id"),
                                                       client_id=self.event.get("cid"),
                                                       marketplace_id=self.event.get("mpid"),
                                                       channel_id=self.event.get("mpid"),
                                                       account_id=self.event.get("acid")
                                                       , stage="create_status_queue")

    def find_queue(self, mpid):
        # ServiceLogger("schedule-status-job").info("In find queue", str(self.__class__.__name__),
        #                                           'main.py', "find_queue",
        #                                           job_id=self.event.get("_id"),
        #                                           client_id=self.event.get("cid"),
        #                                           marketplace_id=self.event.get("mpid"),
        #                                           channel_id=self.event.get("mpid"),
        #                                           account_id=self.event.get("acid")
        #                                           , stage="create_status_queue")
        # server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
        # redis_client = redis.from_url(server_url, decode_responses=True)
        # message = f"{os.getenv('STAGE')}_{int(mpid)}_queue_scrapy"
        # try:
        #     queue_name = redis_client.get(message)
        # except:
        #     queue_name = {}
        return "reco_scrapy"

    def report_running_status(self,report_details):
        try:
            ServiceLogger("schedule-status-job").info("In report running status", str(self.__class__.__name__),
                                                      'main.py', "report_running_status",
                                                      job_id=self.event.get("_id"),
                                                      client_id=self.event.get("cid"),
                                                      marketplace_id=self.event.get("mpid"),
                                                      channel_id=self.event.get("mpid"),
                                                      account_id=self.event.get("acid")
                                                      , stage="create_status_queue")
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
            ServiceLogger("schedule-status-job").error(exception_message, class_name, filename, filename,
                                                       str(exception_traceback.tb_lineno), exception_message,
                                                       job_id=self.event.get("_id"),
                                                       client_id=self.event.get("cid"),
                                                       marketplace_id=self.event.get("mpid"),
                                                       channel_id=self.event.get("mpid"),
                                                       account_id=self.event.get("acid")
                                                       , stage="create_status_queue")

    def report_configs(self):
        try:
            ServiceLogger("schedule-status-job").info("In report config", str(self.__class__.__name__),
                                                      'main.py', "report_configs",
                                                      job_id=self.event.get("_id"),
                                                      client_id=self.event.get("cid"),
                                                      marketplace_id=self.event.get("mpid"),
                                                      channel_id=self.event.get("mpid"),
                                                      account_id=self.event.get("acid")
                                                      , stage="create_status_queue")
            with open(self.report_config, "r") as readfile:
                data = json.loads(readfile.read())
            return data
        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("schedule-status-job").error(exception_message, class_name, filename, filename,
                                                       str(exception_traceback.tb_lineno), exception_message,
                                                       job_id=self.event.get("_id"),
                                                       client_id=self.event.get("cid"),
                                                       marketplace_id=self.event.get("mpid"),
                                                       channel_id=self.event.get("mpid"),
                                                       account_id=self.event.get("acid")
                                                       , stage="create_status_queue")

    def get_report_by_status(self, status, each_report):
        ServiceLogger("schedule-status-job").info("In get report by status", str(self.__class__.__name__),
                                                  'main.py', "get_report_by_status",
                                                  job_id=self.event.get("_id"),
                                                  client_id=self.event.get("cid"),
                                                  marketplace_id=self.event.get("mpid"),
                                                  channel_id=self.event.get("mpid"),
                                                  account_id=self.event.get("acid")
                                                  , stage="create_status_queue")
        try:
            from datetime import timedelta, date
            today = str(date.today() - timedelta(days=0))
            pipeline = [
                {
                    "$match": {
                        "status":
                            {
                                "$in": status
                            },
                        "cid": each_report.get("cid"),
                        "mpid": each_report.get("mpid"),
                        "acid": each_report.get("acid"),
                        "vendor":each_report.get("vendor")
                    }
                },
            ]
            get_status = self.db.aggregate_query(
                collection_name="master_messages",
                pipeline=pipeline
            )
            return get_status
        except Exception as err:
            exception_message = str(err)
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = os.path.split(exception_traceback.tb_frame.f_code.co_filename)[1]
            class_name = str(self.__class__.__name__)
            ServiceLogger("schedule-status-job").error(exception_message, class_name, filename, filename,
                                                       str(exception_traceback.tb_lineno), exception_message,
                                                       job_id=self.event.get("_id"),
                                                       client_id=self.event.get("cid"),
                                                       marketplace_id=self.event.get("mpid"),
                                                       channel_id=self.event.get("mpid"),
                                                       account_id=self.event.get("acid")
                                                       , stage="create_status_queue")
