from dotenv import load_dotenv
load_dotenv()
import base64
import datetime
from datetime import datetime
import json
import os
import sys
from reco_mongodb.mongodbops import MongoDBConnector
from service_logger.service_logger import ServiceLogger

import redis



"""use this class to Schedule the scrapy Request messages """


class RequestScheduleManager:
    def __init__(self, event):
        try:
            self.event = event
            self.db = MongoDBConnector(os.getenv("MONGODB_URL"), os.getenv("MONGODB_SCRAPY_NAME"))
            # if os.getenv("STAGE") == "prod":
            #     self.report_config = os.getenv("REPORT_CONFIG")
            # else:
            self.report_config = "vinreco_Master.json"
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
                                                 , stage="create_status_queue"
                                                 )

    def create_job(self):
        from tasks import scrapy_worker
        ServiceLogger("scrapy_Engine").info(f"In create_request_job func:", '--', "main.py", "status_finder", stage="create_status_queue")
        report_conf = self.report_configs()
        ServiceLogger("scrapy_Engine").info(f"executing get_report_list_with_status fun", '--', "main.py",
                                            "status_finder", stage="create_status_queue")
        get_report_list_with_status = self.get_report_list_with_status(["QQ"], self.event)
        if get_report_list_with_status.alive:
            report_details_id_list = []
            temp_list = []
            for report in get_report_list_with_status:
                if report['report_type'] not in temp_list:
                    # print(report)
                    job_config = list(
                        filter(lambda record: record['report_type'].lower() == report['report_type'].lower(), report_conf))
                    concurrency_flag = job_config[0].get('concurrency')
                    self.delay = job_config[0].get('delay_minuite')
                    if concurrency_flag == 0:
                        """concurrency_flag 0 means we can't run the same script at a time"""
                        ServiceLogger("scrapy_Engine").info(f"executing check_if_any_request_inprogress fun", '--',
                                                            "main.py", "status_finder", stage="create_status_queue")
                        check_delay = self.check_delay(report)
                        if check_delay and self.get_request_validation(report):
                            queue_name = self.find_queue(report.get('mpid'))
                            object_id = report.get('_id')
                            report.update({"_id": str(object_id)})
                            report.update({"action": "Request"})
                            message = json.dumps(report)
                            message_bytes = message.encode('ascii')
                            base64_bytes = base64.b64encode(message_bytes)
                            base64_message = base64_bytes.decode('ascii')
                            ServiceLogger("scrapy_Engine").info(f"{report} sending request", '--', "main.py",
                                                                "status_finder")
                            scrapy_worker.apply_async(args=[base64_message], queue=queue_name)
                            report_details_id_list.append(report.get('_id'))
                            temp_list.append(report.get("report_type"))
                            # print(done)
                            self.db.update_one("master_messages", {'_id': object_id}, {"$set": {'toprocess': 1}})
                            break
                        else:
                            temp_list.append(report.get("report_type"))
                            ServiceLogger("scrapy_Engine").info(f"skiped this request:- \n\n{report}", '--',
                                                                "main.py",
                                                                "status_finder")
                            continue
                    else:
                        check_delay = self.check_delay(report)
                        if check_delay and self.get_login_validation(self.event):
                            queue_name = self.find_queue(report.get('mpid'))
                            object_id = report.get('_id')
                            report.update({"_id": str(object_id)})
                            report.update({"action": "Request"})
                            message = json.dumps(report)
                            message_bytes = message.encode('ascii')
                            base64_bytes = base64.b64encode(message_bytes)
                            base64_message = base64_bytes.decode('ascii')
                            ServiceLogger("scrapy_Engine").info(f"{report} sending request", '--', "main.py",
                                                                "status_finder")

                            scrapy_worker.apply_async(args=[base64_message], queue=queue_name)
                            report_details_id_list.append(report.get('_id'))
                            temp_list.append(report.get("report_type"))
                            # print(done)
                            self.db.update_one("master_messages", {'_id': object_id}, {"$set": {'toprocess': 1}})
                            break
                        else:
                            temp_list.append(report.get("report_type"))
                            ServiceLogger("scrapy_Engine").info(f"skiped this request:- \n\n{report}", '--',
                                                                "main.py", "status_finder")
                            continue
            # print(temp_list)
            self.db.close_connection()
            return {"status_code": 200,
                    "message": f"Successfully Updated object id is : {report_details_id_list}"}
        else:
            self.db.close_connection()
            ServiceLogger("scrapy_Engine").info(f"NO Report with status QQ for this account{self.event}", '--',
                                                "main.py", "status_finder")

    def find_queue(self, mpid):
        # server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
        # redis_client = redis.from_url(server_url, decode_responses=True)
        # message = f"{os.getenv('STAGE')}_{int(mpid)}_queue_scrapy"
        # try:
        #     queue_name = redis_client.get(message)
        # except:
        #     queue_name = {}
        return "reco_scrapy"

    def report_configs(self):
        with open(self.report_config, "r") as ymlfile:
            data = json.loads(ymlfile.read())
        return data

    def check_delay(self, report):
        import datetime
        ServiceLogger("scrapy_Engine").info(f"In check_delay fun", '--', "main.py", "status_finder")

        try:
            delay = self.delay
            ServiceLogger("scrapy_Engine").info(f"executing get_last_request_time fun", '--', "main.py",
                                                "status_finder")

            last_request_time = self.get_last_request_time1(report)
            if last_request_time == "":
                self.set_report_delay(report)
                return True

            delay = datetime.timedelta(minutes=delay)
            to_make_request = last_request_time + delay
            current_datetime = datetime.datetime.today()
            if current_datetime > to_make_request:
                return True
            else:
                ServiceLogger("scrapy_Engine").info(
                    f"you have to wait few minuites, please try again later\n\n{self.event}", '--', "main.py",
                    "status_finder")

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

    def set_report_delay(self,params):
        try:
            REDIS_CONNECTION = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            client = redis.from_url(REDIS_CONNECTION, decode_responses=True)
            if 'seller' in params.get('report_type'):
                message = f"{params.get('acid')}_seller"
            elif 'returns' in params.get('report_type') and params.get("mpid") == 200:
                message = f"{params.get('acid')}_returns"
            else:
                message = f"{params.get('acid')}_{params.get('report_type')}"
            try:
                data1 = json.loads(client.get(message))
            except:
                data1 = {}
            data1.update({"request_time": datetime.now()})
            data = json.dumps(data1, sort_keys=True, default=str)
            client.set(message, data)
        except:
            pass

    def report_running_status(self, report_details):
        try:
            server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            r = redis.from_url(server_url, decode_responses=True)
            if 'seller' in report_details.get('report_type'):
                message = f"{report_details.get('cid')}_{report_details.get('mpid')}_{report_details.get('acid')}_{report_details.get('vendor')}_seller"
            else:
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

    def get_last_request_time1(self, params):
        try:
            ServiceLogger("scrapy_Engine").info(f"In get_last_request_time fun:-", '--', "main.py", "status_finder")
            REDIS_CONNECTION = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            client = redis.from_url(REDIS_CONNECTION, decode_responses=True)
            if 'seller' in params.get('report_type'):
                message = f"{params.get('acid')}_seller"
            elif 'returns' in params.get('report_type') and params.get("mpid") == 200:
                message = f"{params.get('acid')}_returns"
            else:
                message = f"{params.get('acid')}_{params.get('report_type')}"
            try:
                value = json.loads(client.get(message))
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

    def get_last_request_time2(self, report_details):
        try:
            ServiceLogger("scrapy_Engine").info(f"In get_last_request_time fun:-", '--', "main.py", "status_finder")
            server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            r = redis.from_url(server_url, decode_responses=True)
            print(report_details.get('report_type'))

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

    def get_request_validation(self, report):
        try:
            ServiceLogger("scrapy_Engine").info(f"In get_last_request_time fun:-", '--', "main.py", "status_finder")
            server_url = f"redis://{os.getenv('REDIS_CONNECTION')}:6379"
            r = redis.from_url(server_url, decode_responses=True)
            message = f"{report.get('cid')}_{report.get('mpid')}_{report.get('acid')}_{report.get('vendor')}_{report.get('report_type')}"
            print(message)
            try:
                r_status = json.loads(message)['status']
                if r_status == "done" or self.check_delay(report):
                    return True
                else:
                    return False
            except Exception as e:
                print(e)
                return True
        except Exception as e:
            print(f"Error ===================> {e}")
            return True

    def get_report_list_with_status(self, status, each_report):
        ServiceLogger("scrapy_Engine").info(f"In get_report_list_with_status fun", '--', "main.py", "status_finder")

        try:
            from datetime import timedelta, date
            today = str(date.today() - timedelta(days=0))
            limit = 1
            pipeline = [
                {
                    "$match": {
                        "status": {"$in": status},
                        "cid": each_report.get("cid"),
                        "mpid": each_report.get("mpid"),
                        "acid": each_report.get("acid"),
                        "vendor": each_report.get("vendor"),
                        '$or': [{'toprocess': {'$exists': False}}, {'toprocess': {'$eq': 0}}]
                    }
                },
                {
                    "$group": {
                        "_id": "$report_type",
                        "orders": {"$first": "$$ROOT"},
                        "count": {"$sum": 1},
                    }
                },
                {
                    "$replaceRoot": {"newRoot": "$orders"}
                },
                {'$limit': limit}
            ]
            get_report_list_with_status = self.db.aggregate_query(
                collection_name="master_messages",
                pipeline=pipeline
            )
            return get_report_list_with_status
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
                        "acid": each_report.get("acid")
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
