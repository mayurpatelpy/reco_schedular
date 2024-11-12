from celeryapp import app

@app.task()
def scrapy_worker(batch):
    print(batch)