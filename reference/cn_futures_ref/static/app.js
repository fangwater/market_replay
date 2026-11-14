(() => {
  const api = "/api/v1";
  const state = {
    contracts: null,
    notices: null,
    view: "contracts",
    contractVenue: "ALL",
    noticeVenue: "ALL",
    search: "",
    expanded: new Set(),
  };
  const $ = (selector) => document.querySelector(selector);
  const escape = (value) => String(value == null ? "--" : value).replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character]));
  const decimal = (value, digits = 4) => value == null ? "--" : Number(value).toLocaleString("zh-CN", { maximumFractionDigits: digits });
  const rate = (value) => value == null ? "--" : `${Number(value * 100).toLocaleString("zh-CN", { maximumFractionDigits: 4 })}%`;
  const timestamp = (value) => value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "--";
  const date = (value) => value || "日期待核";

  async function request(path, options) {
    const response = await fetch(`${api}${path}`, options);
    if (!response.ok) {
      let message = `请求失败（${response.status}）`;
      try {
        const body = await response.json();
        message = body.error?.message || message;
      } catch (_) {}
      throw new Error(message);
    }
    return response.json();
  }

  function filteredExchanges() {
    if (!state.contracts) return [];
    const needle = state.search.trim().toUpperCase();
    return state.contracts.exchanges.map((exchange) => {
      if (state.contractVenue !== "ALL" && exchange.code !== state.contractVenue) return null;
      const instruments = exchange.instruments.filter((instrument) => {
        const haystack = `${instrument.instrumentId} ${instrument.productId} ${instrument.instrumentName || ""}`.toUpperCase();
        return !needle || haystack.includes(needle);
      });
      return instruments.length ? { ...exchange, instruments } : null;
    }).filter(Boolean);
  }

  function renderNavigation() {
    document.querySelectorAll("[data-view]").forEach((button) => button.classList.toggle("active", button.dataset.view === state.view));
    $("#nav-total").textContent = state.contracts ? state.contracts.summary.instruments : "--";
    $("#nav-rules").textContent = state.notices ? state.notices.summary.pendingRules : "--";
    const contracts = state.view === "contracts";
    const exchanges = contracts ? (state.contracts?.exchanges || []) : (state.notices?.exchanges || []);
    const currentVenue = contracts ? state.contractVenue : state.noticeVenue;
    const total = contracts ? state.contracts?.summary.instruments : state.notices?.announcements.length;
    $("#exchange-nav").innerHTML = `
      <button class="nav-item ${currentVenue === "ALL" ? "active" : ""}" type="button" data-venue="ALL"><span>全部交易所</span><span>${total ?? "--"}</span></button>
      ${exchanges.map((exchange) => `
        <button class="nav-item ${currentVenue === exchange.code ? "active" : ""}" type="button" data-venue="${escape(exchange.code)}">
          <span>${escape(exchange.code)}</span><span>${contracts ? exchange.count : exchange.announcements}</span>
        </button>`).join("")}`;
    document.querySelectorAll("[data-venue]").forEach((button) => button.addEventListener("click", () => {
      if (state.view === "contracts") {
        state.contractVenue = button.dataset.venue;
        renderContracts();
      } else {
        state.noticeVenue = button.dataset.venue;
        renderNoticeView();
      }
    }));
  }

  function renderMetrics(exchanges) {
    const instruments = exchanges.reduce((total, exchange) => total + exchange.instruments.length, 0);
    const futures = exchanges.reduce((total, exchange) => total + exchange.instruments.filter((instrument) => instrument.instrumentType === "FUTURE").length, 0);
    $("#metric-contracts").textContent = instruments.toLocaleString("zh-CN");
    $("#metric-exchanges").textContent = exchanges.length;
    $("#metric-futures").textContent = futures.toLocaleString("zh-CN");
    $("#metric-versions").textContent = Number(state.contracts.summary.instrumentVersions).toLocaleString("zh-CN");
  }

  function instrumentRow(instrument) {
    return `<tr>
      <td><strong>${escape(instrument.instrumentId)}</strong><small>${escape(instrument.instrumentName || instrument.instrumentType)}</small></td>
      <td><strong>${escape(instrument.productId)}</strong><small>${escape(instrument.productName || "--")}</small></td>
      <td>${decimal(instrument.contractMultiplier, 2)}<small>最小变动 ${decimal(instrument.priceTick, 4)}</small></td>
      <td>${decimal(instrument.latestPrice, 4)}<small>昨结算 ${decimal(instrument.previousSettlementPrice, 4)}</small></td>
      <td>${rate(instrument.fees.open.rate)}<small>${decimal(instrument.fees.open.perLot, 4)} / 手</small></td>
      <td>${rate(instrument.fees.close.rate)}<small>${decimal(instrument.fees.close.perLot, 4)} / 手</small></td>
      <td class="today-rate">${rate(instrument.fees.closeToday.rate)}<small>${decimal(instrument.fees.closeToday.perLot, 4)} / 手</small></td>
      <td><small>开始 ${escape(timestamp(instrument.startTs))}</small><small>更新 ${escape(timestamp(instrument.updateTs))}</small></td>
    </tr>`;
  }

  function renderExchanges(exchanges) {
    if (!exchanges.length) {
      $("#exchange-list").innerHTML = '<div class="empty-state">没有符合当前筛选条件的合约。</div>';
      return;
    }
    $("#exchange-list").innerHTML = exchanges.map((exchange) => {
      const expanded = state.expanded.has(exchange.code) || state.search.trim().length > 0 || state.contractVenue !== "ALL";
      const display = expanded ? exchange.instruments : exchange.instruments.slice(0, 8);
      const remaining = exchange.instruments.length - display.length;
      return `<section class="exchange-section" id="exchange-${escape(exchange.code)}">
        <header class="exchange-heading">
          <div><div class="exchange-code">${escape(exchange.code)}</div><h2>${escape(exchange.name)}</h2></div>
          <div class="exchange-summary"><span>${exchange.count} 条期货合约</span><span>${exchange.products.length} 个品种</span></div>
        </header>
        <div class="product-line">${exchange.products.map((product) => `<span>${escape(product)}</span>`).join("")}</div>
        <div class="data-table-wrap"><table class="data-table"><thead><tr><th>合约</th><th>品种</th><th>合约规格</th><th>行情参考</th><th>开仓</th><th>平仓</th><th>平今</th><th>增量记录</th></tr></thead><tbody>${display.map(instrumentRow).join("")}</tbody></table></div>
        ${remaining > 0 ? `<button type="button" class="show-more" data-expand="${escape(exchange.code)}">展开剩余 ${remaining} 条合约</button>` : ""}
        ${expanded && state.contractVenue === "ALL" && !state.search.trim() && exchange.instruments.length > 8 ? `<button type="button" class="show-more secondary" data-collapse="${escape(exchange.code)}">收起该交易所</button>` : ""}
      </section>`;
    }).join("");
    document.querySelectorAll("[data-expand]").forEach((button) => button.addEventListener("click", () => {
      state.expanded.add(button.dataset.expand); renderContracts();
    }));
    document.querySelectorAll("[data-collapse]").forEach((button) => button.addEventListener("click", () => {
      state.expanded.delete(button.dataset.collapse); renderContracts();
    }));
  }

  function renderContracts() {
    if (!state.contracts) return;
    const exchanges = filteredExchanges();
    renderNavigation();
    renderMetrics(exchanges);
    renderExchanges(exchanges);
  }

  function badge(status) {
    const labels = { COMPLETED: "分析完成", FAILED: "分析失败", PENDING: "待审核", VERIFIED: "已审核", REJECTED: "已驳回", INCOMPLETE: "待补充" };
    return `<span class="status-badge status-${escape(String(status || "pending").toLowerCase())}">${escape(labels[status] || "待分析")}</span>`;
  }

  function tiersTable(rule) {
    const tiers = Array.isArray(rule.tiers) ? rule.tiers : [];
    const fee = (value) => String(value).includes("元") ? escape(value) : `${escape(value)} 元/笔`;
    return `<div class="tier-table-wrap"><table class="tier-table"><thead><tr><th>信息量</th><th>OTR ≤ ${escape(rule.otr_threshold)}</th><th>OTR &gt; ${escape(rule.otr_threshold)}</th></tr></thead><tbody>${tiers.map((tier) => `
      <tr><td>${tier.min} - ${tier.max == null ? "以上" : tier.max}</td><td>${fee(tier.rate_otr_le_threshold)}</td><td>${fee(tier.rate_otr_gt_threshold)}</td></tr>`).join("")}</tbody></table></div>`;
  }

  function renderRules() {
    const inVenue = (code) => state.noticeVenue === "ALL" || code === state.noticeVenue;
    const rules = state.notices.rules.filter((rule) => rule.review_status === "PENDING" && inVenue(rule.exchange));
    const incomplete = [];
    state.notices.announcements.filter((notice) => inVenue(notice.venue)).forEach((notice) => {
      const saved = new Set((notice.rules || []).map((rule) => rule.rule_key));
      ((notice.raw_analysis || {}).rules || []).forEach((rule) => {
        if (rule.instrument_type !== "OPTION" && !saved.has(rule.rule_id)) incomplete.push({ rule, notice });
      });
    });
    $("#pending-count").textContent = `${rules.length + incomplete.length} 条`;
    if (!rules.length && !incomplete.length) {
      $("#rule-list").innerHTML = '<div class="empty-state compact">当前没有待审核规则。</div>';
      return;
    }
    const savedRules = rules.map((rule) => `<article class="rule-item">
      <header class="rule-heading">
        <div><span class="venue-tag">${escape(rule.exchange)}</span><h3>${escape((rule.products || []).join(" · "))}</h3></div>
        ${badge(rule.review_status)}
      </header>
      <div class="rule-meta"><span>${escape(rule.instrument_type)}</span><span>${date(rule.effective_from)} 起</span><span>${escape(rule.aggregation_scope)}</span></div>
      ${tiersTable(rule)}
      ${(rule.exemptions || []).length ? `<p class="rule-note">豁免：${escape(rule.exemptions.join("；"))}</p>` : ""}
      <footer class="rule-actions">
        <a href="${escape(rule.source_url)}" target="_blank" rel="noreferrer">查看交易所原文</a>
        <div><button type="button" class="reject-button" data-review="REJECTED" data-rule-id="${rule.id}">驳回</button><button type="button" class="approve-button" data-review="VERIFIED" data-rule-id="${rule.id}">审核通过</button></div>
      </footer>
    </article>`).join("");
    const incompleteRules = incomplete.map(({ rule, notice }) => `<article class="rule-item incomplete-rule">
      <header class="rule-heading">
        <div><span class="venue-tag">${escape(rule.exchange)}</span><h3>${escape((rule.products || []).join(" · "))}</h3></div>
        ${badge("INCOMPLETE")}
      </header>
      <div class="rule-meta"><span>${escape(rule.instrument_type)}</span><span>${date(rule.effective_from)} 起</span><span>${escape(rule.aggregation_scope)}</span></div>
      ${tiersTable(rule)}
      <div class="uncertainties">${(notice.uncertainties || []).map((item) => `<span>${escape(item)}</span>`).join("")}</div>
      <footer class="rule-actions"><a href="${escape(notice.source_url)}" target="_blank" rel="noreferrer">查看交易所原文</a><span>不可执行</span></footer>
    </article>`).join("");
    $("#rule-list").innerHTML = savedRules + incompleteRules;
    document.querySelectorAll("[data-review]").forEach((button) => button.addEventListener("click", () => reviewRule(button)));
  }

  function announcementCard(notice) {
    return `<article class="announcement-item">
        <header class="announcement-heading">
          <div class="announcement-source"><span>${date(notice.published_date)}</span></div>
          ${badge(notice.analysis_status)}
        </header>
        <h3><a href="${escape(notice.source_url)}" target="_blank" rel="noreferrer">${escape(notice.title)}</a></h3>
        ${notice.summary ? `<p class="announcement-summary">${escape(notice.summary)}</p>` : ""}
        ${notice.error_message ? `<p class="error-copy">${escape(notice.error_message)}</p>` : ""}
        ${(notice.uncertainties || []).length ? `<div class="uncertainties">${notice.uncertainties.map((item) => `<span>${escape(item)}</span>`).join("")}</div>` : ""}
        <footer class="announcement-footer">
          <div class="archive-links"><a href="${api}/announcements/${notice.id}/raw" target="_blank">PG 原文</a>${(notice.assets || []).map((asset) => `<a href="${api}/announcement-assets/${asset.id}" target="_blank">${escape(asset.file_name)}</a>`).join("")}</div>
          <span>${((notice.raw_analysis || {}).rules || []).length} 条分析候选 · ${escape(notice.content_sha256.slice(0, 10))}</span>
        </footer>
      </article>`;
  }

  function renderAnnouncements() {
    const items = state.notices.announcements.filter((notice) => state.noticeVenue === "ALL" || notice.venue === state.noticeVenue);
    $("#announcement-count").textContent = `${items.length} 条`;
    const exchanges = state.notices.exchanges.filter((exchange) => state.noticeVenue === "ALL" || exchange.code === state.noticeVenue);
    const sourceResults = state.notices.latestRefresh?.result_json?.collection?.sources || [];
    $("#announcement-list").innerHTML = exchanges.map((exchange) => {
      const exchangeItems = items.filter((notice) => notice.venue === exchange.code);
      const sourceResult = sourceResults.find((source) => source.venue === exchange.code);
      const sourceBlocked = sourceResult?.pages === 0 && (sourceResult.errors || []).length > 0;
      const emptyMessage = sourceBlocked
        ? "该交易所官网当前限制自动采集，公告原文尚未保存；上方规则说明已按官方通知整理。"
        : "尚未保存该交易所的期货相关公告。";
      return `<section class="announcement-exchange-group" id="announcements-${escape(exchange.code)}">
        <header class="announcement-exchange-heading">
          <div><span class="venue-tag">${escape(exchange.code)}</span><h3>${escape(exchange.name)}</h3></div>
          <span>${exchangeItems.length} 条期货公告</span>
        </header>
        <div class="announcement-group-list">${exchangeItems.length ? exchangeItems.map(announcementCard).join("") : `<div class="empty-state compact">${emptyMessage}</div>`}</div>
      </section>`;
    }).join("");
  }

  function renderNoticeView() {
    if (!state.notices) return;
    const inVenue = (code) => state.noticeVenue === "ALL" || code === state.noticeVenue;
    const items = state.notices.announcements.filter((notice) => inVenue(notice.venue));
    const rules = state.notices.rules.filter((rule) => inVenue(rule.exchange));
    const incomplete = items.reduce((total, notice) => {
      const saved = new Set((notice.rules || []).map((rule) => rule.rule_key));
      return total + ((notice.raw_analysis || {}).rules || []).filter((rule) => rule.instrument_type !== "OPTION" && !saved.has(rule.rule_id)).length;
    }, 0);
    $("#metric-announcements").textContent = items.length;
    $("#metric-pending").textContent = rules.filter((rule) => rule.review_status === "PENDING").length + incomplete;
    $("#metric-verified").textContent = rules.filter((rule) => rule.review_status === "VERIFIED").length;
    $("#metric-failed").textContent = items.filter((notice) => notice.analysis_status === "FAILED").length;
    document.querySelectorAll("[data-definition-venue]").forEach((section) => {
      section.classList.toggle("hidden", state.noticeVenue !== "ALL" && section.dataset.definitionVenue !== state.noticeVenue);
    });
    const ai = state.notices.ai;
    $("#ai-status").textContent = ai.available ? `${ai.model} · ${ai.reasoningEffort} · ${ai.provider}` : "分析配置不可用";
    $("#last-refresh").textContent = state.notices.latestRefresh ? `上次手动拉取 ${timestamp(state.notices.latestRefresh.finished_at || state.notices.latestRefresh.started_at)}` : "尚未手动拉取";
    renderRules();
    renderAnnouncements();
    renderNavigation();
  }

  function switchView(view) {
    state.view = view;
    const contracts = view === "contracts";
    $("#contract-view").classList.toggle("hidden", !contracts);
    $("#contract-metrics").classList.toggle("hidden", !contracts);
    $("#contract-actions").classList.toggle("hidden", !contracts);
    $("#announcement-view").classList.toggle("hidden", contracts);
    $("#announcement-actions").classList.toggle("hidden", contracts);
    $("#exchange-label").classList.remove("hidden");
    $("#exchange-label").textContent = contracts ? "交易所" : "公告交易所";
    $("#page-eyebrow").textContent = contracts ? "参考数据库" : "交易所原始公告";
    $("#page-title").textContent = contracts ? "期货参考数据" : "公告与申报费规则";
    renderNavigation();
    if (contracts) renderContracts(); else if (state.notices) renderNoticeView(); else loadNotices().catch(showError);
  }

  function showError(error) {
    $("#connection-status").textContent = "连接失败";
    const target = state.view === "contracts" ? $("#exchange-list") : $("#announcement-list");
    target.innerHTML = `<div class="empty-state">${escape(error.message || "数据暂时无法读取")}</div>`;
    console.error(error);
  }

  async function loadContracts() {
    $("#connection-status").textContent = "正在加载";
    state.contracts = await request("/overview?includeOptions=false");
    $("#connection-status").textContent = "数据已连接";
    renderContracts();
  }

  async function loadNotices() {
    state.notices = await request("/announcements");
    $("#connection-status").textContent = "数据已连接";
    renderNoticeView();
  }

  async function reviewRule(button) {
    const status = button.dataset.review;
    if (status === "REJECTED" && !window.confirm("确认驳回这条候选规则？")) return;
    button.disabled = true;
    try {
      await request(`/message-fee-rules/${button.dataset.ruleId}/review`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ status }),
      });
      await loadNotices();
    } catch (error) {
      showError(error);
    } finally {
      button.disabled = false;
    }
  }

  $("#search-input").addEventListener("input", (event) => { state.search = event.target.value; renderContracts(); });
  document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
  $("#refresh-button").addEventListener("click", () => loadContracts().catch(showError));
  $("#sync-button").addEventListener("click", async (event) => {
    const button = event.currentTarget;
    button.disabled = true;
    button.textContent = "正在同步";
    try {
      await request("/sync", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      await loadContracts();
    } catch (error) {
      showError(error);
    } finally {
      button.disabled = false;
      button.textContent = "同步数据源";
    }
  });
  $("#pull-button").addEventListener("click", async (event) => {
    const button = event.currentTarget;
    const report = $("#operation-report");
    button.disabled = true;
    button.textContent = "正在拉取并分析";
    report.classList.remove("hidden", "report-error");
    report.textContent = "正在访问交易所并分析新公告…";
    try {
      const result = await request("/announcements/refresh", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ pageCount: 5 }) });
      const collection = result.collection;
      report.textContent = `发现 ${collection.relevant} 条相关公告，新增 ${collection.inserted} 条内容版本；完成 ${result.analysis.completed} 条分析，生成 ${result.analysis.rulesInserted} 条候选规则。`;
      await loadNotices();
    } catch (error) {
      report.classList.add("report-error");
      report.textContent = error.message;
    } finally {
      button.disabled = false;
      button.textContent = "拉取并分析";
    }
  });

  const initialView = new URLSearchParams(window.location.search).get("view");
  Promise.all([loadContracts(), loadNotices()])
    .then(() => switchView(["announcement", "announcements"].includes(initialView) ? "announcements" : "contracts"))
    .catch(showError);
})();
