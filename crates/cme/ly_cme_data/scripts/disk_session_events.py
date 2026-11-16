"""Bounded, repeatable RIC event iterators backed by local sorted Parquet."""
import fcntl
import heapq
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

STATUS={'ric':'VARCHAR','product':'VARCHAR','ts_utc_ns':'UBIGINT','exch_hms_ns':'UBIGINT',
        'qualifiers':'VARCHAR','source_order':'UBIGINT'}
CORRECTIONS={'ric':'VARCHAR','published_ns':'UBIGINT','qualifiers':'VARCHAR',
             'source_order':'UBIGINT','source_cf':'VARCHAR','source_db':'VARCHAR'}


def prepare(connection,work,jobs):
    """JSON is parsed and sorted in DuckDB with its fixed memory/spill budget.

    Per-source files are reused by adjacent year groups. A local lock prevents
    two workers building the same index; original JSON and DBs stay immutable.
    """
    result={'status':[],'corrections':[]}
    for job in jobs:
        root=work/'extract'/job['id']
        source=json.loads((root/'state.json').read_text())
        with (root/'event_index.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            for kind,columns,clock in [('status',STATUS,'ts_utc_ns'),('corrections',CORRECTIONS,'published_ns')]:
                path=root/(kind+'.sorted.parquet');receipt=root/(kind+'.index.json')
                identity=dict(version=1,source=source['identity'],source_index=job['source_index'])
                if not (path.exists() and receipt.exists() and json.loads(receipt.read_text())==identity):
                    temporary=path.with_suffix('.parquet.partial')
                    # Fixed types preserve u64 sentinels; empty JSON arrays use
                    # exactly the same schema as non-empty source arrays.
                    specification='{'+','.join(f"'{k}':'{v}'" for k,v in columns.items())+'}'
                    destination="'"+str(temporary).replace("'","''")+"'"
                    connection.execute(f'''COPY (SELECT *,?::UINTEGER source_index
                        FROM read_json(?,format='array',columns={specification})
                        ORDER BY ric,{clock},source_order)
                        TO {destination} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 8192)''',
                        [job['source_index'],str(root/(kind+'.json'))])
                    expected=source.get('status_rows' if kind=='status' else 'correction_rows')
                    if expected is not None:assert pq.ParquetFile(temporary).metadata.num_rows==expected,(job['id'],kind)
                    temporary.replace(path)
                    temp_receipt=receipt.with_suffix('.json.partial');temp_receipt.write_text(json.dumps(identity));temp_receipt.replace(receipt)
                result[kind].append(path)
    return result


def _rows(path,ric,clock,lower,upper):
    source=pq.ParquetFile(path);position=source.schema_arrow.names.index('ric');selected=[]
    for i in range(source.num_row_groups):
        stat=source.metadata.row_group(i).column(position).statistics
        if stat and stat.has_min_max and not stat.min<=ric<=stat.max:continue
        selected.append(i)
    for batch in source.iter_batches(batch_size=8192,row_groups=selected,use_threads=False):
        mask=pc.equal(batch['ric'],ric)
        if lower is not None:mask=pc.and_(mask,pc.greater_equal(batch[clock],pa.scalar(lower,pa.uint64())))
        if upper is not None:mask=pc.and_(mask,pc.less(batch[clock],pa.scalar(upper,pa.uint64())))
        yield from batch.filter(mask).to_pylist()


class Rows:
    """Re-iterable sorted view; no full-archive or per-RIC Python list."""
    def __init__(self,paths,ric,clock='ts_utc_ns',lower=None,upper=None):
        self.paths=paths;self.ric=ric;self.clock=clock;self.lower=lower;self.upper=upper

    def __iter__(self):
        streams=[_rows(path,self.ric,self.clock,self.lower,self.upper) for path in self.paths]
        return heapq.merge(*streams,key=lambda r:(r[self.clock],r['source_index'],r['source_order']))

    def __len__(self):
        return sum(1 for _ in self)
