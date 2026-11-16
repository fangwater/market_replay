import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import publish_after_full_extract as handoff


class HandoffTests(unittest.TestCase):
    def test_dead_extractor_does_not_leave_a_stale_running_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            run=Path(directory);(run/'extract_state.json').write_text(json.dumps(dict(stage='running',completed=1,jobs=2,failed=[])))
            with patch.object(handoff,'RUN',run),patch.object(handoff,'process_identity',side_effect=ProcessLookupError),\
                patch.object(handoff.os,'execv') as execute,patch('sys.argv',['handoff','--pid','123','--start-ticks','456']):
                with self.assertRaises(SystemExit):handoff.main()
                execute.assert_not_called()
            result=json.loads((run/'extract_state.json').read_text())
            self.assertEqual(result['stage'],'failed');self.assertEqual(result['completed'],1)

    def test_failed_extraction_does_not_start_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            run=Path(directory);(run/'extract_state.json').write_text(json.dumps(dict(stage='failed',completed=1,jobs=2,failed=['x'])))
            with patch.object(handoff,'RUN',run),patch.object(handoff,'process_identity',side_effect=ProcessLookupError),\
                patch.object(handoff.os,'execv') as execute,patch('sys.argv',['handoff','--pid','123','--start-ticks','456']):
                with self.assertRaises(SystemExit):handoff.main()
                execute.assert_not_called()
            self.assertEqual(json.loads((run/'handoff_state.json').read_text())['stage'],'failed')

    def test_success_waits_for_exact_process_then_starts_budgeted_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            run=Path(directory);(run/'extract_state.json').write_text(json.dumps(dict(stage='complete',completed=2,jobs=2,failed=[])))
            with patch.object(handoff,'RUN',run),patch.object(handoff,'process_identity',side_effect=[(1,123,456),(1,123,789)]),\
                patch.object(handoff.time,'sleep') as sleep,patch.object(handoff.os,'execv') as execute,\
                patch('sys.argv',['handoff','--pid','123','--start-ticks','456','--workers','2']):
                handoff.main();sleep.assert_called_once_with(5)
                command=execute.call_args.args[1]
                self.assertEqual(command[1],'--');self.assertTrue(command[3].endswith('build_full_tas_klines.py'))
                self.assertEqual(command[-2:],['--workers','2'])


if __name__=='__main__':unittest.main()
