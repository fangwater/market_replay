"""Publication-time LSEG session decoder for calibrated CME product families.

Raw events stay immutable. Auxiliary patches never manufacture an opening,
clear halt on an empty reason, or turn an unknown reason into a known halt.
"""
from collections import defaultdict
from functools import lru_cache
import re
from types import MappingProxyType

FIELDS=re.compile(r'(?:^|;)([^;]*)\[([^\]]+)\]')
CME_ROOTS={'ES','NQ','RTY','YM','GC','CL','SRA','S1R','URO','KWR'}
POLICY_VERSION='2'
RELEVANT={'TRD_STATUS','INST_PHASE','TRD_TYPE','ORD_ENT_ST','HALT_REASN','HALT_RSN',
    'PERIOD_CDE','PERIOD_CD2','SECUR_ST','MKT_ST_IND','PRC_QL_CD','STAT_IND'}


@lru_cache(maxsize=65536)
def qualifiers(text):
    """Missing FID, explicitly blank FID and conflicting values remain distinct."""
    result=defaultdict(set)
    for value,name in FIELDS.findall(text):result[name].add(value.strip().strip('"').strip())
    return MappingProxyType({key:frozenset(values) for key,values in result.items()})


def _decode(text,previous='UNKNOWN',product=None,cme_scope=None):
    fields=qualifiers(text)
    is_cme=product in CME_ROOTS if cme_scope is None else bool(cme_scope)
    terminal=previous=='EXPIRED' and is_cme
    result=dict(action='NO_CHANGE',session_state=previous,reason='UNSPECIFIED',
        is_trading=True if previous=='TRADING' else False if previous in {'CLOSED','PRE_OPEN','HALTED','NOT_TRADING','EXPIRED'} else None,
        issue_code=None)
    if FIELDS.sub('',text).strip('; \t\r\n'):
        result.update(action='UNCLASSIFIED',session_state='EXPIRED' if terminal else 'UNKNOWN',
            is_trading=False if terminal else None,reason='INSTRUMENT_EXPIRATION' if terminal else 'UNSPECIFIED',issue_code='unparsed_qualifier_text')
        return result
    conflicts=[name for name,values in fields.items() if name in RELEVANT and len(values)>1]
    if conflicts:
        result.update(action='UNCLASSIFIED',session_state='EXPIRED' if terminal else 'UNKNOWN',
            is_trading=False if terminal else None,reason='INSTRUMENT_EXPIRATION' if terminal else 'UNSPECIFIED',issue_code='conflicting_qualifier_values:'+','.join(sorted(conflicts)))
        return result
    get=lambda name:next(iter(fields[name])) if name in fields else None
    status=get('TRD_STATUS');trade=get('TRD_TYPE');phase=get('INST_PHASE')
    closed=trade=='4' and phase=='C'
    preopen=trade=='21' and phase=='O'
    trading=trade=='15' or trade=='17' and phase=='T'
    if previous=='EXPIRED' and is_cme:
        result.update(session_state='EXPIRED',is_trading=False,reason='INSTRUMENT_EXPIRATION')
        if trading or preopen or status=='H':result['issue_code']='session_marker_after_expiration'
        return result
    if status=='H':
        result.update(action='HALT',session_state='HALTED',is_trading=False,reason='EXPLICIT_HALT')
        if trading:result['issue_code']='halt_and_trading_in_same_patch'
        if get('HALT_REASN')=='4':result['issue_code']='expiration_and_explicit_halt_in_same_patch'
        return result
    if sum([closed,preopen,trading])>1:
        result.update(action='UNCLASSIFIED',session_state='UNKNOWN',is_trading=None,issue_code='contradictory_session_markers')
        return result
    if not is_cme:
        if previous=='HALTED' and status not in ('N',):
            result.update(action='NO_CHANGE',session_state='HALTED',is_trading=False,reason='EXPLICIT_HALT')
            return result
        if status=='N':
            result.update(action='HALT_RELEASE',session_state='UNKNOWN',is_trading=None)
            return result
        result.update(action='UNCLASSIFIED',session_state='UNKNOWN',is_trading=None,issue_code='uncalibrated_product')
        return result
    if phase not in (None,'','C','O','T') or trade not in (None,'','2','4','15','17','18','21','24'):
        result.update(action='UNCLASSIFIED',session_state='UNKNOWN',is_trading=None,
            issue_code='unmapped_session_code:'+str(trade)+'/'+str(phase))
        return result
    if get('HALT_REASN')=='4':
        # CME Tag 327=4 is instrument expiration. The LSEG native binding
        # is calibrated by 52 same-RIC Databento events; restrict to these roots.
        if trading or get('HALT_RSN')=='SS':
            result.update(action='UNCLASSIFIED',session_state='UNKNOWN',is_trading=None,
                issue_code='expiration_conflicts_with_live_session_marker')
        else:
            result.update(action='EXPIRE',session_state='EXPIRED',is_trading=False,reason='INSTRUMENT_EXPIRATION')
        return result
    if trade=='2' or get('HALT_RSN')=='SS':
        # Native 2 matches HALT/RECOVERY in the calibrated 2026 overlap.
        # An unavailable instrument during weekend setup does not establish
        # a new interruption of a trading session.
        result.update(action='HALT' if trade=='2' else 'NOT_AVAILABLE',
            session_state='HALTED' if previous in {'TRADING','HALTED'} else previous if previous in {'CLOSED','PRE_OPEN'} else 'NOT_TRADING',
            is_trading=False,reason='SYSTEM_SUSPENDED' if get('HALT_RSN')=='SS' else 'NATIVE_SUSPEND')
    elif trade=='18' and phase=='C':
        result.update(action='NOT_AVAILABLE',session_state='CLOSED',is_trading=False)
    elif closed:
        result.update(action='CLOSE',session_state='CLOSED',is_trading=False)
    elif preopen:
        result.update(action='PRE_OPEN',session_state='HALTED' if previous=='HALTED' else 'PRE_OPEN',
            is_trading=False,reason='HALT_RECOVERY' if previous=='HALTED' else 'UNSPECIFIED')
    elif trading:
        result.update(action='OPEN',session_state='TRADING',is_trading=True)
    elif status=='N':
        # "Normal" clears the explicit halt flag, not the session schedule.
        result.update(action='HALT_RELEASE',session_state='UNKNOWN' if previous=='HALTED' else previous)
        result['is_trading']=None if result['session_state']=='UNKNOWN' else result['is_trading']
    elif get('ORD_ENT_ST')=='N':
        result['action']='NO_CANCEL'
    elif get('SECUR_ST') in ('5','6'):
        result['action']='IMPLIED_ON' if get('SECUR_ST')=='5' else 'IMPLIED_OFF'
    elif trade=='24':
        result['action']='PRE_CROSS'
    if status not in (None,'','H','N'):result['issue_code']='unknown_trading_status:'+status
    if (get('HALT_REASN') not in (None,'','0','2') or get('HALT_RSN') not in (None,'','NH')) and get('HALT_RSN')!='SS':
        result['issue_code']=result['issue_code'] or 'halt_reason_without_explicit_halt'
        if result['action']=='NO_CHANGE' and previous=='TRADING':
            result.update(session_state='UNKNOWN',is_trading=None)
    unknown=set(fields)-RELEVANT
    if unknown:result['issue_code']=result['issue_code'] or 'unmapped_qualifier_fields:'+','.join(sorted(unknown))
    if trade not in (None,'','2','4','15','17','18','21','24'):
        result['issue_code']=result['issue_code'] or 'unmapped_native_trade_type:'+trade
    return result


@lru_cache(maxsize=65536)
def _cached_decode(text,previous,product,cme_scope):
    return tuple(_decode(text,previous,product,cme_scope).items())


def decode(text,previous='UNKNOWN',product=None,cme_scope=None):
    # Full-store status streams repeat a small set of patches millions of
    # times. Scope overrides make the product irrelevant to interpretation.
    # Return a fresh dict so callers cannot modify the cached result.
    return dict(_cached_decode(text,previous,product if cme_scope is None else None,cme_scope))


def official_event(record):
    """Reviewed official messages can take effect only once public and effective."""
    required=['source_url','published_ns','effective_ns','products','session_state','evidence_sha256']
    if any(key not in record for key in required):raise ValueError('official supplement lacks source/time/scope/evidence')
    if not record['source_url'].startswith(('https://www.cmegroup.com/','https://cmegroup.com/','https://x.com/CMEGroup/status/')):
        raise ValueError('supplement must identify a CME official source')
    if not record['products'] or not all(isinstance(p,str) and p for p in record['products']):raise ValueError('empty official product scope')
    if record['session_state'] not in {'TRADING','HALTED','CLOSED','PRE_OPEN'}:raise ValueError('unknown official state')
    if not re.fullmatch('[0-9a-f]{64}',record['evidence_sha256']):raise ValueError('invalid official evidence digest')
    return dict(record,ts_utc_ns=max(int(record['published_ns']),int(record['effective_ns'])),
        source='CME_OFFICIAL',action='OFFICIAL_'+record['session_state'],
        is_trading=record['session_state']=='TRADING')
