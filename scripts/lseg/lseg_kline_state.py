"""LSEG-only session intervals. No calendar, DBN, IB or external status input."""
import heapq
from itertools import groupby

import pyarrow as pa

from lseg_session_status import decode, qualifiers

SCHEMA=pa.schema([('start_ns',pa.int64()),('end_ns',pa.int64()),
    ('is_trading',pa.bool_()),('session_id',pa.int64()),('phase',pa.string())])


def intervals(rows, product, start, end, gaps=(),cme_scope=None):
    """Apply patches in source order, but expose only the final same-time state.

    A missing archive invalidates carried state. Auxiliary patches cannot restore
    it; the next explicit phase is required. A missing reason never means zero.
    """
    ordered=sorted(rows,key=lambda r:(int(r['ts_utc_ns']),r.get('source_index',0),r.get('source_order',0)))
    return ordered_intervals(ordered,product,start,end,gaps,cme_scope)


def ordered_intervals(rows,product,start,end,gaps=(),cme_scope=None):
    """Sorted disk iterators need only the current timestamp and coalesced states."""
    gap_starts={max(start,int(lo)) for lo,hi in gaps if lo<end and hi>start}
    events=((int(r['ts_utc_ns']),1,r) for r in rows if int(r['ts_utc_ns'])<end)
    markers=((at,0,None) for at in sorted(gap_starts))
    timeline=heapq.merge(events,markers,key=lambda r:(r[0],r[1]))
    previous='UNKNOWN';phase='UNKNOWN';native=None;cursor=start;session=0;out=[]
    def emit(row):
        if out and all(out[-1][k]==row[k] for k in ('phase','is_trading','session_id')):
            assert out[-1]['end_ns']==row['start_ns'];out[-1]['end_ns']=row['end_ns']
        else:
            if len(out)>=1_000_000:raise RuntimeError('more than one million actual session intervals in one RIC; finer disk partition required')
            out.append(row)
    for at,group in groupby(timeline,key=lambda r:r[0]):
        if at>=end:break
        patches=(r for _,_,r in group if r is not None)
        old_phase=phase
        if at>=start and at>cursor:
            emit(dict(start_ns=cursor,end_ns=at,is_trading=True if phase=='TRADING' else None if phase=='UNKNOWN' else False,
                session_id=session if phase=='TRADING' else None,phase=phase))
            cursor=at
        if at in gap_starts:previous='UNKNOWN';phase='UNKNOWN';native=None
        in_gap=any(lo<=at<hi for lo,hi in gaps)
        actions=[];explicit_halt=False;bad=False
        prior_phase=phase
        for row in patches:
            fields=qualifiers(row['qualifiers'])
            if 'HALT_REASN' in fields and len(fields['HALT_REASN'])==1:
                native=next(iter(fields['HALT_REASN']))
            explicit_halt|=fields.get('TRD_STATUS')=={'H'}
            result=decode(row['qualifiers'],previous,product,cme_scope=cme_scope)
            previous=result['session_state'];actions.append(result['action'])
            bad|=result['action']=='UNCLASSIFIED'
        if previous=='EXPIRED':phase='EXPIRED'
        elif in_gap:previous='UNKNOWN';phase='UNKNOWN';native=None
        elif bad:phase='UNKNOWN'
        elif previous=='TRADING':phase='TRADING'
        elif explicit_halt:phase='HALTED'
        elif previous=='HALTED':
            phase='HALTED' if prior_phase in {'TRADING','HALTED'} else prior_phase if prior_phase=='SCHEDULED_CLOSED' else 'UNKNOWN'
        elif any(a in {'CLOSE','PRE_OPEN','NOT_AVAILABLE','HALT'} for a in actions):
            # Scheduled PRE_OPEN after a real halt is still that interruption.
            if prior_phase=='HALTED' and 'PRE_OPEN' in actions and 'CLOSE' not in actions:phase='HALTED'
            elif native=='0':phase='SCHEDULED_CLOSED'
            elif native in {'1','2','6'} and prior_phase in {'TRADING','HALTED'}:phase='HALTED'
            elif prior_phase=='SCHEDULED_CLOSED':phase=prior_phase
            else:phase='UNKNOWN'
        elif previous=='UNKNOWN':phase='UNKNOWN'
        # Auxiliary/duplicate rows preserve the established phase and reason.
        if phase=='TRADING' and old_phase!='TRADING':session+=1
    if cursor<end:
        emit(dict(start_ns=cursor,end_ns=end,is_trading=True if phase=='TRADING' else None if phase=='UNKNOWN' else False,
            session_id=session if phase=='TRADING' else None,phase=phase))
    return pa.Table.from_pylist(out,schema=SCHEMA)


def lifetime(start, end, rows, first_dates, expiry_dates, first_trade=None,last_trade=None, minute=60_000_000_000):
    """Use observed LSEG evidence, not a UTC interpretation of a trading date.

    HRD first dates only establish existence before the requested year. A new
    Sunday opening can precede its Monday HRD trading-day label. All real prints
    survive even when reference dates or expiry status conflict with them.
    """
    # Auxiliary field updates can precede actual instrument session markers by
    # days (e.g. the May FX serial listing preparation). They do not establish
    # a listed lifetime. A definite phase or a real print is the lower bound.
    observed=[int(r['ts_utc_ns']) for r in rows if decode(r['qualifiers'],'UNKNOWN',r['product'])['action'] in
        {'OPEN','PRE_OPEN','CLOSE','HALT','NOT_AVAILABLE','EXPIRE'}]
    if first_trade is not None:observed.append(first_trade)
    mature=bool(any(d<start for d in first_dates) or any(t<start for t in observed))
    lo=start if mature else max(start,min(observed,default=start)//minute*minute)
    expiry=[int(r['ts_utc_ns']) for r in rows if decode(r['qualifiers'],'UNKNOWN',r['product'])['action']=='EXPIRE']
    candidates=expiry+list(expiry_dates)
    cutoff=min(candidates) if candidates else None
    hi=min(end,(cutoff+minute-1)//minute*minute) if cutoff is not None else end
    issues=[]
    if first_trade is not None:
        lo=min(lo,first_trade//minute*minute)
        if cutoff is not None and last_trade is not None and last_trade>=cutoff:
            issues.append(dict(issue='trade_after_lseg_expiry',cutoff_ns=cutoff,last_trade_ns=last_trade))
        if last_trade is not None:hi=max(hi,(last_trade//minute+1)*minute)
    return lo,hi,cutoff,issues
