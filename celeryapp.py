from __future__ import absolute_import
from celery import Celery
from celery.schedules import crontab

app = Celery('reco-client-backend-scrapy',include=['tasks'])
app.config_from_object("celeryconfig")
app.conf.update(
    result_expires=3600,
)
app.conf.beat_schedule = {
    'schedule-reco-scrapy-download-every-3-minute': {
        'task': 'tasks.schedule_scrapy_download_worker',
        'schedule': crontab(minute='*/3'),
    },
    'schedule-reco-scrapy-status-every-5-minute': {
        'task': 'tasks.schedule_scrapy_status_worker',
        'schedule': crontab(minute='*/5'),
    },
    'schedule-reco-scrapy-requests-every-5-minute': {
        'task': 'tasks.schedule_scrapy_requests_worker',
        'schedule': crontab(minute='*/5'),
    }
}

if __name__ == '__main__':
    app.start()
