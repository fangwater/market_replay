import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cn_futures_ref import codex_ai


class CodexAiTests(unittest.TestCase):
    def test_loads_current_codex_style_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config.toml").write_text(
                'model_provider = "OpenAI"\n[model_providers.OpenAI]\nbase_url = "http://api.test/v1"\n',
                encoding="utf-8",
            )
            (root / "auth.json").write_text(
                json.dumps({"OPENAI_API_KEY": "secret"}), encoding="utf-8"
            )
            with patch.dict(os.environ, {"CN_FUTURES_REF_CODEX_HOME": directory}):
                settings = codex_ai.load_settings()
        self.assertEqual(settings.responses_url, "http://api.test/v1/responses")
        self.assertEqual(settings.api_key, "secret")

    def test_request_pins_model_effort_and_structured_output(self):
        payload = codex_ai.build_request({
            "venue": "CFFEX", "title": "公告", "published_date": "2026-09-02",
            "source_url": "https://example.test/notice", "extracted_text": "正文",
        }, [])
        self.assertEqual(payload["model"], "gpt-5.6-sol")
        self.assertEqual(payload["reasoning"], {"effort": "high"})
        self.assertFalse(payload["store"])
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")


if __name__ == "__main__":
    unittest.main()
