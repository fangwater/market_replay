import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import maintain_cme_notices as m


SOURCE=dict(id='example',kind='known_official_post',url='https://cdn.syndication.twimg.com/tweet-result?id=1',
    source_url='https://x.com/CMEGroup/status/1',expected_author_id='15935591',expected_post_id='1',enabled=True,
    cache_ttl_seconds=100)


def body(text='Notice',likes=1,author='15935591'):
    return json.dumps(dict(id_str='1',user=dict(id_str=author),created_at='2025-11-28T13:18:30.000Z',
        text=text,favorite_count=likes,isEdited=False,isStaleEdit=False)).encode()


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        (m.WORK/'tests').mkdir(parents=True,exist_ok=True)
        self.temp=tempfile.TemporaryDirectory(dir=m.WORK/'tests');self.addCleanup(self.temp.cleanup)
        self.work=Path(self.temp.name)

    def test_valid_cache_avoids_repeated_network_requests(self):
        state,issue=m.collect(SOURCE,{},self.work,fetch=lambda u:body(),now=1000)
        self.assertIsNone(issue);self.assertFalse(state['complete_discovery'])
        def fail(url):raise AssertionError('unexpected repeated request')
        again,issue=m.collect(SOURCE,state,self.work,fetch=fail,now=1001)
        self.assertEqual(again['status'],'cached_known_document');self.assertIsNone(issue)

    def test_403_persists_circuit_and_preserves_evidence(self):
        state,_=m.collect(SOURCE,{},self.work,fetch=lambda u:body(),now=1000)
        def blocked(url):raise HTTPError(url,403,'Forbidden',{},io.BytesIO())
        failure,issue=m.collect(SOURCE,state,self.work,fetch=blocked,now=1200)
        self.assertEqual(failure['status'],'blocked_403');self.assertTrue(Path(failure['evidence_path']).exists())
        def fail(url):raise AssertionError('403 must not be retried')
        again,issue=m.collect(SOURCE,failure,self.work,fetch=fail,now=1300)
        self.assertEqual(again['status'],'blocked_403');self.assertIsNotNone(issue)

    def test_disabled_discovery_source_is_not_a_successful_poll(self):
        state,issue=m.collect(dict(SOURCE,enabled=False),{},self.work,fetch=lambda u: (_ for _ in ()).throw(AssertionError()))
        self.assertEqual(state['status'],'disabled');self.assertIsNotNone(issue)

    def test_identity_mismatch_is_rejected(self):
        state,issue=m.collect(SOURCE,{},self.work,fetch=lambda u:body(author='other'),now=1000)
        self.assertEqual(state['status'],'fetch_failed');self.assertIn('author',issue['problem'])

    def test_engagement_changes_are_not_content_edits(self):
        self.assertEqual(m.parse_post(body(likes=1),SOURCE)['semantic_sha256'],m.parse_post(body(likes=20),SOURCE)['semantic_sha256'])

    def test_changed_notice_stays_unresolved_on_a_cache_hit(self):
        state,_=m.collect(SOURCE,{},self.work,fetch=lambda u:body(),now=1000)
        changed,issue=m.collect(SOURCE,state,self.work,fetch=lambda u:body(text='Different reopening time'),now=1200)
        self.assertIsNotNone(issue);self.assertTrue(changed['semantic_changed'])
        cached,issue=m.collect(SOURCE,changed,self.work,now=1201)
        self.assertIsNotNone(issue);self.assertFalse(cached['automatically_applied'])

    def test_http_403_is_not_retried_but_transient_503_is(self):
        class Response:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self,n):return b'ok'
        with patch.object(m.urllib.request,'urlopen',side_effect=HTTPError('https://example.com',403,'Forbidden',{},None)) as request:
            with self.assertRaises(HTTPError):m.fetch_url('https://example.com')
            self.assertEqual(request.call_count,1)
        with patch.object(m.time,'sleep'),patch.object(m.urllib.request,'urlopen',side_effect=[HTTPError('https://example.com',503,'Unavailable',{},None),Response()]) as request:
            self.assertEqual(m.fetch_url('https://example.com'),b'ok');self.assertEqual(request.call_count,2)


if __name__=='__main__':unittest.main()
