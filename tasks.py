from celeryapp import app
from reco_scrapy.scrapy_schedule_manager import ScrapyScheduleManager
from service_logger.service_logger import ServiceLogger

@app.task()
def scrapy_worker(batch):
    print(batch)
