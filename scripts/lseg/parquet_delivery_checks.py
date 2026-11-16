"""Small deterministic value-readback samples and cheap complete footer checks."""
from datetime import datetime, date

import pyarrow.parquet as pq


def sampled_paths(reports):
    result=set()
    for report in reports:
        for kind in (1,3):
            parts=sorted((p for p in report['parts'] if p['kind']==kind),
                         key=lambda p:(p.get('utc_date') or '',p['ric'],p['path']))
            if parts:
                result.update(parts[i]['path'] for i in {0,len(parts)//2,len(parts)-1})
    return result


def check_footer(path, part=None, reference=None):
    footer=pq.ParquetFile(path)
    if reference is not None:
        assert footer.metadata.num_rows==reference.metadata.num_rows,path
        assert footer.schema_arrow.equals(reference.schema_arrow,check_metadata=True),path
    if part is not None:
        if part['rows'] is not None:
            assert footer.metadata.num_rows==part['rows'],path
        names=footer.schema_arrow.names
        ric_index=names.index('ric');time_index=names.index('ts_utc_ns')
        day=part.get('utc_date')
        if day:
            days=(datetime.strptime(day,'%Y%m%d').date()-date(1970,1,1)).days
            start=days*86_400_000_000_000;end=start+86_400_000_000_000
        for i in range(footer.num_row_groups):
            group=footer.metadata.row_group(i)
            ric=group.column(ric_index).statistics
            assert ric is not None and ric.has_min_max and ric.min==ric.max==part['ric'],path
            if day:
                stamp=group.column(time_index).statistics
                assert stamp is not None and stamp.has_min_max and start<=stamp.min<=stamp.max<end,path
    return footer
