import hashlib, json
def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
def dataset_dict(d):
    return {'id':d.id, 'name':d.name, 'source':d.source, 'rows':len(d.records), 'checksum':d.checksum, 'created_at':d.created_at}
def job_dict(j):
    return {k:getattr(j,k) for k in ['id','name','dataset_id','params','status','result','error','approved','created_at','finished_at']}
def report_dict(r):
    return {k:getattr(r,k) for k in ['id','name','job_id','destination','interval_seconds','next_run','enabled']}
def delivery_dict(d, view=None):
    return {
        'id': d.id, 'report_id': d.report_id, 'created_at': d.created_at,
        'scheduled_for': d.scheduled_for, 'completed_at': d.completed_at,
        'status': d.status, 'checksum': d.checksum, 'filename': d.filename,
        'destination': d.destination,
        'csv_filename': d.csv_filename, 'json_filename': d.json_filename,
        'csv_checksum': d.csv_checksum, 'json_checksum': d.json_checksum,
        'package_state': view['state'] if view else ('complete' if d.status == 'complete' else 'pending'),
        'message': view['message'] if view else '',
        'checks': view['checks'] if view else {},
    }
