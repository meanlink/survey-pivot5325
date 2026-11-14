import os
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import select
from src.config.database import get_db
from src.config.settings import ARTIFACT_DIR
from src.models import Report, Delivery
from src.schemas.report import ReportInput
from src.middlewares.auth import current_user, admin_only
from src.services import reports
from src.services.audit import record
from src.utils.serialization import report_dict, delivery_dict
router=APIRouter()
@router.get('/reports')
def listing(db=Depends(get_db),u=Depends(current_user)):
    return [report_dict(r) for r in db.scalars(select(Report).order_by(Report.id.desc()))]
@router.post('/reports',status_code=201)
def create(data:ReportInput,db=Depends(get_db),u=Depends(admin_only)):
    r=reports.create(db,data); record(db,u.email,'report.schedule',{'id':r.id,'destination':r.destination}); db.commit()
    return report_dict(r)
@router.post('/reports/{id}/toggle')
def toggle(id:int,db=Depends(get_db),u=Depends(admin_only)):
    r=db.get(Report,id)
    if not r:raise HTTPException(404,'分发计划不存在')
    r.enabled=not r.enabled;record(db,u.email,'report.toggle',{'id':id,'enabled':r.enabled});db.commit()
    return report_dict(r)
@router.get('/deliveries')
def deliveries(db=Depends(get_db),u=Depends(current_user)):
    rows=[delivery_dict(d,reports.verify(d)) for d in db.scalars(select(Delivery).order_by(Delivery.id.desc()).limit(100))]
    return rows
@router.get('/deliveries/{id}')
def delivery_detail(id:int,db=Depends(get_db),u=Depends(current_user)):
    d=db.get(Delivery,id)
    if not d:raise HTTPException(404,'归档不存在')
    return delivery_dict(d,reports.verify(d))
@router.post('/deliveries/{id}/repair')
def repair(id:int,db=Depends(get_db),u=Depends(admin_only)):
    d=db.get(Delivery,id)
    if not d:raise HTTPException(404,'归档不存在')
    ok,message=reports.repair(db,d)
    record(db,u.email,'delivery.repair',{'id':id,'ok':ok,'message':message})
    db.commit()
    return {**delivery_dict(d,reports.verify(d)),'repair_message':message}
@router.get('/deliveries/{id}/download')
def download(id:int,format:str='csv',db=Depends(get_db),u=Depends(current_user)):
    d=db.get(Delivery,id)
    if not d:raise HTTPException(404,'归档不存在')
    if format not in ('csv','json'):raise HTTPException(422,'支持 CSV 或 JSON')
    view=reports.verify(d)
    if view['state']=='pending':
        raise HTTPException(409,'结果包尚未完整写入，后台会恢复同次分发后再下载')
    if view['state']!='complete':
        if reports.legacy_archive(d):
            raise HTTPException(409,'旧归档文件损坏：'+view['message']+'。旧归档没有分发时快照，不能自动修复；请沿用其余完好副本或重新建立分发计划')
        raise HTTPException(409,'归档文件损坏：'+view['message']+'。请在分发页按当时分析快照修复整包后再下载')
    if reports.legacy_archive(d):
        filename=d.filename+('.json' if format=='json' else '')
        path=os.path.join(ARTIFACT_DIR,filename)
        media='application/json' if format=='json' else 'text/csv; charset=utf-8'
        return FileResponse(path,filename=filename,media_type=media)
    path=os.path.join(reports.package_dir(d),'report.json' if format=='json' else 'report.csv')
    return FileResponse(path,filename=f'report-{d.report_id}-delivery-{d.id}.{format}',
        media_type='application/json' if format=='json' else 'text/csv; charset=utf-8')
