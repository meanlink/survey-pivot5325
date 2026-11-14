import time, json, logging
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report, Delivery
from src.services.business import analyze
from src.services.reports import claim, publish, advance, recoverable, cleanup_staging
from src.services.audit import record
logging.basicConfig(level=logging.INFO)
def execute(job_id):
    with Session() as db:
        j=db.get(Job,job_id)
        if not j or j.status!='queued':return
        j.status='running'; db.commit()
        try:
            d=db.get(Dataset,j.dataset_id)
            result=analyze(d.records,j.params)
            json.dumps(result,allow_nan=False)
            j.result=result;j.status='completed';j.finished_at=time.time()
            record(db,'worker','job.completed',{'id':j.id})
        except Exception as e:
            logging.exception('Analysis failed')
            j.status='failed';j.error=str(e)[:990];j.finished_at=time.time()
            record(db,'worker','job.failed',{'id':j.id,'error':j.error})
        db.commit()


def publish_pending(db, delivery):
    ok = publish(db, delivery)
    if ok:
        report = db.get(Report, delivery.report_id)
        if report and delivery.scheduled_for is not None:
            advance(db, report, delivery.scheduled_for)
        record(db, 'worker', 'report.delivered', {'id': delivery.report_id, 'delivery': delivery.id})
    else:
        record(db, 'worker', 'report.publish_failed', {'id': delivery.report_id, 'delivery': delivery.id, 'error': delivery.error})
    return ok


def recover():
    """Resume runs claimed before an interruption; never create a second run."""
    cleanup_staging()
    with Session() as db:
        ids=[d.id for d in recoverable(db)]
    for delivery_id in ids:
        with Session() as db:
            d=db.get(Delivery,delivery_id)
            if d:publish_pending(db,d)
            db.commit()


def tick():
    with Session() as db:
        queued=list(db.scalars(select(Job.id).where(Job.status=='queued').limit(3)))
    for job_id in queued:execute(job_id)
    # Claim and commit a due run before any file work. A crash mid-package then
    # leaves a pending row (same planned due time) that recovery can finish.
    claimed=[]
    with Session() as db:
        for r in db.scalars(select(Report).where(Report.enabled==True,Report.next_run<=time.time())):
            d=claim(db,r)
            if not d:continue
            if d.status=='complete':
                # The run already entered the inbox (for example after a
                # restart); only the schedule still needs to move forward.
                advance(db,r,d.scheduled_for)
            else:claimed.append(d.id)
        db.commit()
    # Retry previously claimed runs first, then publish this tick's claims.
    with Session() as db:
        pending=[d.id for d in recoverable(db)]
    for delivery_id in list(dict.fromkeys(pending+claimed)):
        with Session() as db:
            d=db.get(Delivery,delivery_id)
            if d and d.status!='complete':publish_pending(db,d)
            db.commit()
def main():
    r=Redis.from_url(REDIS_URL,socket_timeout=3)
    # This Compose stack has a single worker. A crash must not strand its jobs
    # or leave a due run without a complete package.
    with Session() as db:
        for j in db.scalars(select(Job).where(Job.status=='running')):
            j.status='queued';record(db,'worker','job.recovered',{'id':j.id})
        db.commit()
    try:
        recover()
    except Exception:
        logging.exception('Delivery recovery failed; will retry in tick')
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item=r.blpop('analysis:queue',timeout=1)
            if item:execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry');time.sleep(2)
if __name__=='__main__':main()
