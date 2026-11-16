#!/usr/bin/env python3
"""Maintain known official evidence; blocked discovery never looks healthy."""
import argparse
from datetime import datetime,timezone
import fcntl
import hashlib
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
WORK=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_notices')
RUN=ROOT/'artifacts/cme_notices_maintenance'
PROBLEMS=Path('/mnt/nfs/30.100_cta_data/lseg/meta/problems/cme_notices')


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.partial')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');temp.replace(path)


def fetch_url(url):
    """Bounded retries only for transient faults; never retry a 403."""
    for attempt in range(2):
        try:
            request=urllib.request.Request(url,headers={'User-Agent':'ly-cme-data-notice-maintenance/1.0','Accept':'application/json'})
            with urllib.request.urlopen(request,timeout=15) as response:
                data=response.read(2_000_001)
                if len(data)>2_000_000:raise ValueError('official response exceeds bounded size')
                return data
        except urllib.error.HTTPError as error:
            error.close()
            if error.code not in (429,500,502,503,504) or attempt==1:raise
            time.sleep(1)
        except (urllib.error.URLError,TimeoutError):
            if attempt==1:raise
            time.sleep(1)


def parse_post(data,source):
    post=json.loads(data)
    if str(post.get('user',{}).get('id_str'))!=source['expected_author_id']:raise ValueError('official author ID mismatch')
    if str(post.get('id_str'))!=source['expected_post_id']:raise ValueError('official post ID mismatch')
    at=datetime.fromisoformat(post['created_at'].replace('Z','+00:00'))
    if at.tzinfo is None:raise ValueError('official publication lacks timezone')
    if not post.get('text'):raise ValueError('official post text missing')
    semantic=dict(author_id=source['expected_author_id'],post_id=source['expected_post_id'],
        published_utc=at.astimezone(timezone.utc).isoformat(),text=post['text'],
        is_edited=post.get('isEdited'),is_stale_edit=post.get('isStaleEdit'),edit_control=post.get('edit_control'))
    # Engagement counters and profile images do not constitute an amended notice.
    semantic_hash=hashlib.sha256(json.dumps(semantic,sort_keys=True).encode()).hexdigest()
    return dict(semantic,semantic_sha256=semantic_hash,source_url=source['source_url'])


def collect(source,prior,work,fetch=fetch_url,now=None):
    now=time.time() if now is None else now
    base=dict(id=source['id'],kind=source['kind'],url=source['url'])
    if not source.get('enabled',True):
        return dict(prior,**base,status='disabled',complete_discovery=False),dict(source=source['id'],url=source['url'],
            problem=source.get('disabled_reason','源已停用，未请求'),diagnostic=source.get('diagnostic'))
    if prior.get('status')=='blocked_403' and prior.get('url')==source['url']:
        return prior,dict(source=source['id'],url=source['url'],problem='此前403已暂停自动请求；尚未获得恢复访问的证据')
    if source['kind']!='known_official_post':
        return dict(base,status='unsupported_channel',complete_discovery=False),dict(source=source['id'],url=source['url'],
            problem='尚未接入允许使用的新公告发现渠道，不冒充采集已完整')
    cached=prior.get('evidence_path')
    if cached and now-prior.get('fetched_at_epoch',0)<source.get('cache_ttl_seconds',86400):
        path=Path(cached)
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest()==prior.get('evidence_sha256'):
            pending=dict(source=source['id'],url=source['source_url'],problem='公告变更尚未重新核定，继续保留问题') if prior.get('semantic_changed') else None
            return dict(prior,status='cached_known_document',complete_discovery=False),pending
    try:
        data=fetch(source['url']);post=parse_post(data,source)
        digest=hashlib.sha256(data).hexdigest();path=work/'evidence'/(digest+'.json')
        if not path.exists():
            path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_suffix('.partial');temp.write_bytes(data);temp.replace(path)
        changed=bool(prior.get('semantic_changed') or prior.get('semantic_sha256') and prior['semantic_sha256']!=post['semantic_sha256']
            or post.get('is_edited') or post.get('is_stale_edit'))
        state=dict(base,**post,status='fetched_known_document',fetched_at_epoch=now,evidence_path=str(path),
            evidence_sha256=digest,complete_discovery=False,semantic_changed=changed,
            automatically_applied=False)
        problem=dict(source=source['id'],url=source['source_url'],problem='已知公告原文或编辑状态变化，保留新旧证据，尚未重新核定生效时间和范围') if changed else None
        return state,problem
    except Exception as error:
        status='blocked_403' if isinstance(error,urllib.error.HTTPError) and error.code==403 else 'fetch_failed'
        if isinstance(error,urllib.error.HTTPError):error.close()
        state=dict(prior,**base,status=status,complete_discovery=False,last_error=str(error),last_attempt_epoch=now)
        return state,dict(source=source['id'],url=source['url'],http_status=getattr(error,'code',None),
            problem=str(error),cached_evidence_preserved=bool(cached))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',type=Path,default=CONFIG / 'cme_notices.json')
    args=parser.parse_args();RUN.mkdir(parents=True,exist_ok=True);WORK.mkdir(parents=True,exist_ok=True)
    with (RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        config=json.loads(args.config.read_text());assert config['version']==1
        states=[]
        for source in config['sources']:
            assert re.fullmatch('[A-Za-z0-9_]+',source['id'])
            state_path=RUN/'sources'/(source['id']+'.json');prior=json.loads(state_path.read_text()) if state_path.exists() else {}
            state,problem=collect(source,prior,WORK);save(state_path,state);states.append(state)
            problem_path=PROBLEMS/(source['id']+'.json')
            if problem:save(problem_path,problem)
            else:problem_path.unlink(missing_ok=True)
        summary=dict(status='partial',new_notice_discovery_ready=False,scope='known configured documents only; no claim of complete or current incident coverage',
            known_documents_available=sum(s['status'] in ('cached_known_document','fetched_known_document') for s in states),
            unavailable_sources=[s['id'] for s in states if s['status'] not in ('cached_known_document','fetched_known_document')])
        save(RUN/'state.json',summary);print(json.dumps(summary,ensure_ascii=False))
        return 2 if summary['unavailable_sources'] else 0


if __name__=='__main__':raise SystemExit(main())
