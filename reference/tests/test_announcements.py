import unittest

from bs4 import BeautifulSoup

from cn_futures_ref.announcements import BASELINE_NOTICES, SOURCES, _article, _asset_urls, discover_notices, parse_notice


class AnnouncementParserTests(unittest.TestCase):
    def test_discovers_only_relevant_exchange_notices(self):
        html = """
        <ul>
          <li><a href="./202609/t20260901_123.html" title="关于调整申报费收费标准的通知">费用</a><span>2026-09-01</span></li>
          <li><a href="./202609/t20260901_126.html" title="关于股指期权实施梯度申报费的通知">期权费用</a><span>2026-09-01</span></li>
          <li><a href="./202609/t20260902_124.html" title="关于交割仓库的公告">仓库</a></li>
          <li><a href="./202609/t20260903_125.html" title="上海国际能源交易中心发布关于调整申报费的通知">能源</a></li>
        </ul>
        """
        notices = discover_notices(
            SOURCES[1], html, "https://www.shfe.com.cn/publicnotice/notice/"
        )
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]["venue"], "SHFE")
        self.assertEqual(notices[0]["published_date"], "2026-09-01")

    def test_keeps_cffex_futures_fee_and_skips_option_only_notice(self):
        html = """
        <ul>
          <li><a href="/cn/jystz/20260430/47723.html">关于股指期货实施梯度申报费有关事项的通知</a><span>2026-04-30</span></li>
          <li><a href="/cn/jystz/20260430/47722.html">关于股指期权实施梯度申报费有关事项的通知</a><span>2026-04-30</span></li>
        </ul>
        """
        notices = discover_notices(SOURCES[0], html, "http://www.cffex.com.cn/cn/jystz_2.html")
        self.assertEqual([notice["title"] for notice in notices], [
            "关于股指期货实施梯度申报费有关事项的通知"
        ])

    def test_parses_article_body_and_visible_publish_date(self):
        html = """
        <html><head>
          <meta name="ArticleTitle" content="申报费测试公告">
          <meta name="PubDate" content="2026-09-02 17:00:00">
        </head><body>
          <div class="InfoTitle"><PUBLISHTIME>2026-08-31</PUBLISHTIME></div>
          <div class="InfoContent"><p>信息量=报单和撤单笔数之和。</p></div>
        </body></html>
        """.encode()
        notice = {
            "venue": "GFEX", "title": "旧标题", "url": "http://www.gfex.com.cn/test.shtml",
            "published_date": None,
        }
        parsed, text, assets, errors = parse_notice(
            html, "text/html", notice, notice["url"], retries=0
        )
        self.assertEqual(parsed["title"], "申报费测试公告")
        self.assertEqual(parsed["published_date"], "2026-08-31")
        self.assertIn("信息量", text)
        self.assertEqual(assets, [])
        self.assertEqual(errors, [])

    def test_discovers_cffex_fee_table_image_in_notice_body(self):
        soup = BeautifulSoup("""
          <body><img src="/r/cms/www/default/images/logo.png">
          <div class="jysggnr"><p>费率标准</p><img src="/u/cms/www/202604/fee.png"></div></body>
        """, "html.parser")
        article = _article(soup)
        self.assertEqual(article.get("class"), ["jysggnr"])
        self.assertEqual(_asset_urls(article, "http://www.cffex.com.cn/cn/test.html"), [
            "http://www.cffex.com.cn/u/cms/www/202604/fee.png"
        ])

    def test_tracks_baseline_fee_notices_for_four_exchanges(self):
        self.assertEqual(
            {notice["venue"] for notice in BASELINE_NOTICES},
            {"SHFE", "INE", "DCE", "CZCE"},
        )
        self.assertTrue(all("申报费" in notice["title"] for notice in BASELINE_NOTICES))

    def test_discovers_ine_uploaded_fee_table_image(self):
        soup = BeautifulSoup(
            '<div class="InfoContent"><img src="/upload/20240522/fee.JPG"></div>',
            "html.parser",
        )
        self.assertEqual(_asset_urls(
            _article(soup), "https://www.ine.cn/publicnotice/notice/test.html"
        ), ["https://www.ine.cn/upload/20240522/fee.JPG"])


if __name__ == "__main__":
    unittest.main()
