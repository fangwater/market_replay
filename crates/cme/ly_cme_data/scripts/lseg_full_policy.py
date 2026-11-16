"""Reference and coverage policy for all raw RICs and all stored TAS years."""
from lseg_session_status import decode

DAY=86_400_000_000_000
MINUTE=60_000_000_000
CME_VENUES={'cme','cbot','comex','nymex'}


def window(ric,product,venue,start,end,status,hrd,first_trade,last_trade,reported_last=None):
    """No numeric year expansion: match a reference lifetime to observed prints.

    Date-only first trading labels tolerate the preceding UTC Sunday. Distinct
    reference expiry candidates remain unresolved, never borrowed across cycles.
    Without reliable expiry, stop padding at the last observed source evidence.
    """
    is_cme=venue in CME_VENUES
    refs=[r for r in hrd if r.get('first') is not None and r.get('expiry') is not None
        and r['first']<=first_trade+DAY and r['expiry']>=first_trade-DAY]
    expiries={r['expiry'] for r in refs};issues=[]
    if len(expiries)>1:issues.append(dict(ric=ric,issue='conflicting_lseg_lifetimes',detail=str(sorted(expiries))))
    expiry=next(iter(expiries)) if len(expiries)==1 else None
    first_phase=None;prior_phase=False;native_expiry=None;last_status=last_trade
    for row in status:
        at=int(row['ts_utc_ns'])
        parsed=decode(row['qualifiers'],'UNKNOWN',product,cme_scope=is_cme)
        if parsed['action'] in {'OPEN','PRE_OPEN','CLOSE','HALT','NOT_AVAILABLE','EXPIRE'}:
            prior_phase|=at<start
            if at>=start:first_phase=at if first_phase is None else min(first_phase,at)
        if parsed['action']=='EXPIRE':native_expiry=at if native_expiry is None else min(native_expiry,at)
        if start<=at<end:last_status=max(last_status,at)
    if native_expiry is not None:expiry=min(expiry,native_expiry) if expiry is not None else native_expiry
    mature=bool(any(r['first']<start for r in refs) or prior_phase)
    lo=start if mature else max(start,min(first_trade,first_phase if first_phase is not None else first_trade)//MINUTE*MINUTE)
    if expiry is not None:hi=min(end,(expiry+MINUTE-1)//MINUTE*MINUTE)
    else:
        if reported_last is not None:last_status=max(last_status,reported_last)
        hi=min(end,(last_status//MINUTE+1)*MINUTE)
        issues.append(dict(ric=ric,issue='no_unique_lseg_expiry',detail='padding bounded by last observed LSEG evidence; no external definition/calendar'))
    lo=min(lo,first_trade//MINUTE*MINUTE)
    hi=max(hi,(last_trade//MINUTE+1)*MINUTE)
    if expiry is not None and last_trade>=expiry:
        issues.append(dict(ric=ric,issue='trade_after_lseg_expiry',detail=f'expiry={expiry}; last_trade={last_trade}; actual prints retained'))
    return lo,hi,issues
