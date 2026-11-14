import os, csv, io, hashlib, json, time, logging, fcntl, shutil, uuid
from fastapi import HTTPException
from sqlalchemy import select, update, insert
from sqlalchemy.exc import IntegrityError
from src.models import Job, Report, Delivery
from src.config.settings import ARTIFACT_DIR

log = logging.getLogger(__name__)

STAGING_PREFIX = '.staging-'
LOCK_DIR = '.locks'


def _now():
    return time.time()


def create(db, data):
    j = db.get(Job, data.job_id)
    if not j or j.status != 'completed' or not j.approved:
        raise HTTPException(409, '请先选择已完成且获批准的分析结果')
    r = Report(name=data.name, job_id=j.id, destination=data.destination,
               interval_seconds=data.interval_seconds,
               next_run=_now() + data.delay_seconds, enabled=True)
    db.add(r)
    db.flush()
    return r


def snapshot_job(job):
    """Freeze the approved analysis at the moment a due run is claimed.

    Repair must rebuild from this snapshot only: never re-run the analysis and
    never borrow a newer result.
    """
    return {
        'job': job.name,
        'job_id': job.id,
        'params': job.params,
        'result': job.result,
        'finished_at': job.finished_at,
    }


def _safe_cell(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value)
    return "'" + text if text.startswith(('=', '+', '-', '@', '\t', '\r')) else text


def render_csv(result):
    out = io.StringIO()
    out.write('﻿')
    table = result.get('table', {}) if isinstance(result, dict) else {}
    columns = table.get('columns', [])
    writer = csv.writer(out)
    writer.writerow([_safe_cell(column['label']) for column in columns])
    for row in table.get('rows', []):
        writer.writerow([_safe_cell(row.get(column['key'], '')) for column in columns])
    return out.getvalue().encode('utf-8')


def render_json(delivery, csv_bytes):
    snapshot = delivery.snapshot or {}
    payload = {
        'delivery_id': delivery.id,
        'report_id': delivery.report_id,
        'scheduled_for': delivery.scheduled_for,
        'created_at': delivery.created_at,
        'destination': delivery.destination,
        'job': snapshot.get('job'),
        'job_id': snapshot.get('job_id'),
        'params': snapshot.get('params'),
        'result': snapshot.get('result'),
        'checksum': hashlib.sha256(csv_bytes).hexdigest(),
    }
    return json.dumps(payload, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2).encode('utf-8')


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def package_dir(delivery):
    return os.path.join(ARTIFACT_DIR, f'report-{delivery.report_id}-delivery-{delivery.id}')


def package_names(delivery):
    base = f'report-{delivery.report_id}-delivery-{delivery.id}'
    return base + '/report.csv', base + '/report.json'


def _paths(delivery):
    root = package_dir(delivery)
    return root, os.path.join(root, 'report.csv'), os.path.join(root, 'report.json')


def _lock(delivery):
    os.makedirs(os.path.join(ARTIFACT_DIR, LOCK_DIR), exist_ok=True)
    handle = open(os.path.join(ARTIFACT_DIR, LOCK_DIR, f'delivery-{delivery.id}.lock'), 'a+')
    fcntl.flock(handle, fcntl.LOCK_EX)
    return handle


def _unlock(handle):
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


def _read_sha(path):
    if not os.path.isfile(path):
        return None
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _check(path, expected):
    exists = os.path.isfile(path)
    actual = _read_sha(path) if exists else None
    return {'path': os.path.basename(path), 'exists': exists, 'checksum': actual,
            'match': bool(exists and expected and actual == expected)}


def legacy_archive(delivery):
    return delivery.scheduled_for is None and delivery.snapshot is None and bool(delivery.filename)


def verify(delivery):
    """Check both files and both recorded checksums without changing anything."""
    if legacy_archive(delivery):
        csv_path = os.path.join(ARTIFACT_DIR, delivery.filename)
        json_path = csv_path + '.json'
        csv_check = _check(csv_path, delivery.checksum)
        json_exists = os.path.isfile(json_path)
        json_ok = False
        json_checksum = _read_sha(json_path) if json_exists else None
        if json_exists:
            try:
                with open(json_path, 'r', encoding='utf-8') as handle:
                    payload = json.load(handle)
                json_ok = payload.get('checksum') == delivery.checksum and isinstance(payload.get('result'), dict)
            except (OSError, ValueError):
                json_ok = False
        complete = csv_check['match'] and json_ok
        message = '' if complete else '旧归档文件缺失或内容与归档指纹不符'
        return {
            'state': 'complete' if complete else 'corrupt', 'legacy': True,
            'message': message,
            'checks': {'csv': {'exists': csv_check['exists'], 'checksum': csv_check['checksum'], 'match': csv_check['match']},
                       'json': {'exists': json_exists, 'checksum': json_checksum, 'match': json_ok}},
        }
    root, csv_path, json_path = _paths(delivery)
    if delivery.status != 'complete':
        return {
            'state': 'pending', 'legacy': False,
            'message': delivery.error or '结果包尚未完整写入，后台会恢复同次分发',
            'checks': {'csv': {'exists': os.path.isfile(csv_path), 'checksum': _read_sha(csv_path), 'match': False},
                       'json': {'exists': os.path.isfile(json_path), 'checksum': _read_sha(json_path), 'match': False}},
        }
    csv_check = _check(csv_path, delivery.csv_checksum)
    json_check = _check(json_path, delivery.json_checksum)
    complete = csv_check['match'] and json_check['match']
    if complete:
        message = '两份文件与各自校验值一致'
    else:
        missing = []
        mismatch = []
        for label, check in (('CSV', csv_check), ('JSON', json_check)):
            if not check['exists']:
                missing.append(label)
            elif not check['match']:
                mismatch.append(label)
        parts = []
        if missing:
            parts.append('、'.join(missing) + ' 文件缺失')
        if mismatch:
            parts.append('、'.join(mismatch) + ' 校验值不匹配')
        message = '；'.join(parts) + '，可从分发时分析快照修复整包'
    return {
        'state': 'complete' if complete else 'corrupt', 'legacy': False,
        'message': message,
        'checks': {'csv': {'exists': csv_check['exists'], 'checksum': csv_check['checksum'], 'match': csv_check['match']},
                   'json': {'exists': json_check['exists'], 'checksum': json_check['checksum'], 'match': json_check['match']}},
    }


def _write_package(delivery, csv_bytes, json_bytes, expected_csv=None, expected_json=None):
    """Stage both files, verify their checksums from disk, then publish as one unit."""
    root, csv_path, json_path = _paths(delivery)
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    staging = os.path.join(ARTIFACT_DIR, f'{STAGING_PREFIX}{delivery.id}-{uuid.uuid4().hex}')
    os.makedirs(staging)
    try:
        staged_csv = os.path.join(staging, 'report.csv')
        staged_json = os.path.join(staging, 'report.json')
        with open(staged_csv, 'wb') as handle:
            handle.write(csv_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        with open(staged_json, 'wb') as handle:
            handle.write(json_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        # The package only enters the inbox after both files are present and
        # their checksums match what will be recorded.
        if _read_sha(staged_csv) != _sha(csv_bytes) or _read_sha(staged_json) != _sha(json_bytes):
            raise OSError('暂存文件校验失败')
        if expected_csv and _sha(csv_bytes) != expected_csv:
            raise ValueError('CSV 内容与归档校验值不一致，拒绝覆盖')
        if expected_json and _sha(json_bytes) != expected_json:
            raise ValueError('JSON 内容与归档校验值不一致，拒绝覆盖')
        if os.path.isdir(root):
            trash = os.path.join(ARTIFACT_DIR, f'{STAGING_PREFIX}old-{delivery.id}-{uuid.uuid4().hex}')
            os.replace(root, trash)
            try:
                os.replace(staging, root)
            finally:
                shutil.rmtree(trash, ignore_errors=True)
        else:
            os.replace(staging, root)
        staging = None
    finally:
        if staging and os.path.isdir(staging):
            shutil.rmtree(staging, ignore_errors=True)


def publish(db, delivery):
    """Write and publish one due run. Safe to retry after a crash."""
    handle = _lock(delivery)
    try:
        # Another worker may have finished this run while we waited for the
        # file lock; re-read the row so we never republish over a complete run.
        try:
            db.refresh(delivery)
        except Exception:
            pass
        if delivery.status == 'complete':
            # The run already entered the inbox. Later file damage is surfaced
            # for explicit repair rather than silently rewriting the archive.
            return True
        if not delivery.snapshot:
            delivery.error = '缺少分发时分析快照，无法生成结果包'
            return False
        delivery.last_attempt_at = _now()
        csv_bytes = render_csv((delivery.snapshot or {}).get('result') or {})
        json_bytes = render_json(delivery, csv_bytes)
        if delivery.csv_checksum and _sha(csv_bytes) != delivery.csv_checksum:
            delivery.error = '重建内容与归档指纹不一致，拒绝覆盖'
            return False
        if delivery.json_checksum and _sha(json_bytes) != delivery.json_checksum:
            delivery.error = '重建内容与归档指纹不一致，拒绝覆盖'
            return False
        _write_package(delivery, csv_bytes, json_bytes, delivery.csv_checksum, delivery.json_checksum)
        delivery.csv_filename, delivery.json_filename = package_names(delivery)
        delivery.filename = delivery.csv_filename
        delivery.csv_checksum = _sha(csv_bytes)
        delivery.json_checksum = _sha(json_bytes)
        delivery.status = 'complete'
        delivery.completed_at = delivery.completed_at or _now()
        delivery.error = None
        return True
    except Exception as error:
        log.exception('Delivery package publish failed')
        delivery.error = str(error)[:500]
        return False
    finally:
        _unlock(handle)


def repair(db, delivery):
    """Rebuild the whole package from the frozen snapshot, preserving identity."""
    if legacy_archive(delivery):
        raise HTTPException(409, '旧归档没有分发时快照，只能核验不能自动修复；请沿用原归档或重新建立分发计划')
    if not delivery.snapshot:
        raise HTTPException(409, '缺少分发时分析快照，无法修复此结果包')
    if delivery.status != 'complete':
        # Nothing was published for this claimed run yet, so generating the
        # package from its snapshot is a completion, not an overwrite.
        ok = publish(db, delivery)
        if not ok:
            raise HTTPException(409, delivery.error or '结果包恢复失败，请稍后重试')
        return True, '已恢复同一次到期的结果包，原编号和计划到期时间不变'
    if not delivery.csv_checksum or not delivery.json_checksum:
        raise HTTPException(409, '归档没有完整的双校验值，无法证明修复内容与原文一致')
    handle = _lock(delivery)
    try:
        try:
            db.refresh(delivery)
        except Exception:
            pass
        view = verify(delivery)
        if view['state'] == 'complete':
            return True, '结果包完整，无需修复'
        csv_bytes = render_csv((delivery.snapshot or {}).get('result') or {})
        json_bytes = render_json(delivery, csv_bytes)
        if _sha(csv_bytes) != delivery.csv_checksum or _sha(json_bytes) != delivery.json_checksum:
            raise HTTPException(409, '当时分析快照重建的内容与归档校验值不一致，已保留损坏现场')
        _write_package(delivery, csv_bytes, json_bytes, delivery.csv_checksum, delivery.json_checksum)
        delivery.status = 'complete'
        delivery.error = None
        delivery.completed_at = delivery.completed_at or _now()
        return True, '已按原编号、原计划到期时间和分发时分析快照修复整包'
    finally:
        _unlock(handle)


def claim(db, report, now=None):
    """Claim this report's due run, creating at most one delivery row.

    ``scheduled_for`` is the planned due time and takes part in the unique key,
    so a restart or concurrent worker cannot archive the same run twice.
    """
    if report.next_run is None:
        return None
    due_at = report.next_run
    if now is not None and due_at > now + 1e-6:
        return None
    job = db.get(Job, report.job_id)
    if not job or job.status != 'completed' or not job.approved:
        return None
    existing = db.scalar(select(Delivery).where(
        Delivery.report_id == report.id, Delivery.scheduled_for == due_at))
    if existing:
        return existing
    values = {
        'report_id': report.id, 'created_at': _now(), 'scheduled_for': due_at,
        'status': 'pending', 'checksum': '', 'filename': '', 'destination': report.destination,
        'snapshot': snapshot_job(job),
    }
    _insert_once(db, values)
    return db.scalar(select(Delivery).where(
        Delivery.report_id == report.id, Delivery.scheduled_for == due_at))


def _insert_once(db, values):
    """Insert a claim, letting the database unique key decide a race winner."""
    dialect = db.get_bind().dialect.name
    if dialect == 'postgresql':
        from sqlalchemy.dialects.postgresql import insert as dialect_insert
    elif dialect == 'sqlite':
        from sqlalchemy.dialects.sqlite import insert as dialect_insert
    else:
        dialect_insert = None
    if dialect_insert:
        statement = dialect_insert(Delivery).values(**values).on_conflict_do_nothing(
            index_elements=['report_id', 'scheduled_for'])
        db.execute(statement)
    else:
        try:
            with db.begin_nested():
                db.execute(insert(Delivery).values(**values))
        except IntegrityError:
            pass


def advance(db, report, scheduled_for):
    """Move to the next slot after this run, never backwards.

    One planned due time produces exactly one result package. If the worker was
    down across several intervals, it publishes the pending due run and then
    resumes on the original grid instead of flooding the inbox with a package
    per missed interval.
    """
    interval = report.interval_seconds
    target = scheduled_for + interval
    now = _now()
    if interval > 0 and target <= now:
        missed = int((now - scheduled_for) // interval) + 1
        target = scheduled_for + missed * interval
    db.execute(update(Report).where(
        Report.id == report.id, Report.next_run <= scheduled_for
    ).values(next_run=target))


def deliver(db, report):
    """Immediate delivery used by demo seeding; follows the same run semantics."""
    delivery = claim(db, report)
    if not delivery:
        return None
    if publish(db, delivery):
        advance(db, report, delivery.scheduled_for)
        return delivery
    return delivery


def recoverable(db, retry_after=5.0):
    """Runs that were claimed but never fully published, oldest first."""
    cutoff = _now() - retry_after
    return list(db.scalars(select(Delivery).where(
        Delivery.status == 'pending',
        (Delivery.last_attempt_at.is_(None)) | (Delivery.last_attempt_at <= cutoff),
    ).order_by(Delivery.id)))


def cleanup_staging():
    """Remove half-written staging directories left by an interrupted process."""
    if not os.path.isdir(ARTIFACT_DIR):
        return
    cutoff = _now() - 60
    for name in os.listdir(ARTIFACT_DIR):
        path = os.path.join(ARTIFACT_DIR, name)
        if not name.startswith(STAGING_PREFIX) or not os.path.isdir(path):
            continue
        try:
            if os.path.getmtime(path) < cutoff:
                shutil.rmtree(path, ignore_errors=True)
        except OSError:
            pass
