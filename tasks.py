from celeryapp import app
from service_logger.service_logger import ServiceLogger
from reco_scrapy.scrapy_schedule_manager import ScrapyScheduleManager




@app.task()
def scrapy_worker(batch):
    print(batch)

@app.task()
def schedule_scrapy_worker(message):
    ServiceLogger("scrapy_Engine").info(f"In Tasks queue with message {message}", '--', "main.py", "Schedule_Download")
    schedule_manager = ScrapyScheduleManager(message)
    schedule_job = schedule_manager.schedule()
    ServiceLogger("scrapy_Engine").info(f"Schedule {message} Job Successfully", '--', "main.py", "Schedule_Download")

# @app.task()
# def schedule_scrapy_status_worker():
#     ServiceLogger("scrapy_Engine").info(f"In Tasks status", '--', "main.py", "Schedule_Download")
#     schedule_manager = ScrapyScheduleManager("STATUS")
#     schedule_job = schedule_manager.schedule()
#     ServiceLogger("scrapy_Engine").info(f"Schedule Satus Job Successfully", '--', "main.py", "Schedule_Download")
#
# @app.task()
# def schedule_scrapy_requests_worker():
#     ServiceLogger("scrapy_Engine").info(f"In Tasks Request", '--', "main.py", "Schedule_Download")
#     schedule_manager = ScrapyScheduleManager("REQUEST")
#     schedule_job = schedule_manager.schedule()
#     ServiceLogger("scrapy_Engine").info(f"Schedule Requests Job Successfully", '--', "main.py", "Schedule_Download")