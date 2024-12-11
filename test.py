from reco_scrapy.scrapy_schedule_manager import ScrapyScheduleManager


if __name__ == '__main__':
    scheduler = ScrapyScheduleManager("FLIPKART_DAILY")
    scheduler.schedule()