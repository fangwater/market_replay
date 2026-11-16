import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

import bounded_json_events as events
from resource_runtime import LAUNCHER, bounded_workers, supervised, process_identity


class ResourceTests(unittest.TestCase):
    def test_stale_supervisor_metadata_does_not_bypass_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            lock=Path(directory)/'resource.lock'
            lock.write_text(json.dumps(dict(pid=os.getpid(),start_ticks=process_identity(os.getpid())[2])))
            with patch('resource_runtime.RUNTIME_LOCK',lock):self.assertFalse(supervised())

    def test_worker_budget_is_not_per_machine(self):
        self.assertEqual(bounded_workers('2'),2)
        for value in (0,-1,3,32):
            with self.assertRaises(ValueError):bounded_workers(value)


@unittest.skipUnless(LAUNCHER.is_file() and not supervised(), 'requires a built bin outside another project task')
class ProgramEntryTests(unittest.TestCase):
    def test_config_reports_program_budget_without_admin_dependency(self):
        r=subprocess.run([str(LAUNCHER),'--check'],capture_output=True,text=True,timeout=10,check=True)
        report=json.loads(r.stdout)
        self.assertFalse(report['kernel_hard_limit'])
        self.assertEqual(report['memory_budget_bytes'],64*1024**3)
        self.assertEqual(report['memory_stop_bytes'],60*1024**3)
        self.assertLessEqual(len(report['cpu_affinity']),32)

    def test_nested_workers_share_supervisor_and_independent_tasks_are_excluded(self):
        script="""import json,os,subprocess,sys
sys.path.insert(0,sys.argv[1])
from resource_runtime import enter,supervised
enter()
assert supervised() and os.environ['OPENBLAS_NUM_THREADS']=='1'
child=subprocess.run([sys.argv[2],'--check'],capture_output=True,text=True,check=True)
assert json.loads(child.stdout)['supervisor_pid']==os.getppid()
print('READY',flush=True)
sys.stdin.readline()
"""
        first=subprocess.Popen([str(LAUNCHER),'--',sys.executable,'-c',script,str(Path(__file__).parent),str(LAUNCHER)],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            self.assertEqual(first.stdout.readline().strip(),'READY')
            second=subprocess.run([str(LAUNCHER),'--check'],capture_output=True,text=True,timeout=10)
            self.assertNotEqual(second.returncode,0)
            self.assertIn('another independent project task',second.stderr)
            self.assertIsNone(first.poll())
            stdout,stderr=first.communicate('\n',timeout=10)
            self.assertEqual(first.returncode,0,stderr)
        finally:
            if first.poll() is None:first.terminate();first.communicate(timeout=10)

    def test_wrapped_command_error_code_is_preserved(self):
        r=subprocess.run([str(LAUNCHER),'--',sys.executable,'-c','raise SystemExit(7)'],capture_output=True,text=True,timeout=10)
        self.assertEqual(r.returncode,7,r.stderr)

    def test_duckdb_import_does_not_create_a_machine_sized_thread_pool(self):
        script="""from pathlib import Path
import duckdb
threads=int(next(s for s in Path('/proc/self/status').read_text().splitlines() if s.startswith('Threads:')).split()[1])
assert threads<=8,threads
duckdb.close()
c=duckdb.connect(config={'threads':1,'memory_limit':'2GB'})
assert c.execute("select current_setting('threads')").fetchone()[0]==1
c.close()
"""
        env=dict(os.environ);env.pop('LD_PRELOAD',None)
        r=subprocess.run([str(LAUNCHER),'--',sys.executable,'-c',script],capture_output=True,text=True,timeout=20,env=env)
        self.assertEqual(r.returncode,0,r.stderr)

    def test_short_lived_child_processes_do_not_fail_the_monitor(self):
        script="import subprocess\nfor _ in range(500): subprocess.run(['/bin/true'],check=True)"
        r=subprocess.run([str(LAUNCHER),'--',sys.executable,'-c',script],capture_output=True,text=True,timeout=20)
        self.assertEqual(r.returncode,0,r.stderr)

    def test_member_process_name_may_contain_non_utf8_and_parentheses(self):
        script="""import ctypes,time
libc=ctypes.CDLL(None,use_errno=True)
name=b'cme'+bytes([255])+b')(name'
assert libc.prctl(15,ctypes.c_char_p(name),0,0,0)==0
time.sleep(0.8)
"""
        r=subprocess.run([str(LAUNCHER),'--',sys.executable,'-c',script],capture_output=True,text=True,timeout=10)
        self.assertEqual(r.returncode,0,r.stderr)


class StreamingEventsTests(unittest.TestCase):
    def read(self,text,chunk_size=7):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'events.json';path.write_text(text,encoding='utf-8')
            return list(events.iter_events(path,chunk_size))

    def test_chunk_boundaries_preserve_unicode_order_duplicates_and_u64(self):
        source=[{'ric':'测试\\"', 'ts':2**64-1, 'qualifiers':None, 'nested':['a',i//2]}
                for i in range(1000)]
        result=self.read(json.dumps(source,ensure_ascii=False))
        self.assertEqual([row for row,size in result],source)
        self.assertTrue(all(size>0 for row,size in result))
        self.assertEqual(self.read(' \n [ ] \n'),[])

    def test_truncated_or_malformed_input_is_not_silently_dropped(self):
        for text in ('','{}','[{}','[{},]','[{},','[{} {}]','[1]','[] {}'):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):self.read(text)

    def test_record_and_group_budgets_stop_before_oversized_collection(self):
        with patch.object(events,'MAX_RECORD_CHARS',32):
            with self.assertRaises(ValueError):self.read(json.dumps([{'x':'a'*100}]))
        with patch.object(events,'MAX_GROUP_EVENTS',2),patch.object(events,'MAX_GROUP_CHARS',20):
            budget=events.EventBudget();budget.account(10);budget.account(10)
            with self.assertRaisesRegex(RuntimeError,'no events were discarded'):budget.account(1)
            budget=events.EventBudget()
            with self.assertRaises(RuntimeError):budget.account(21)


if __name__=='__main__':unittest.main()
