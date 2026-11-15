(() => {
  const API = "/api/v1";
  const state = {
    summary: null,
    view: "definition",
    product: "ALL",
    definitionDate: "",
    statusDate: "",
    definitionPage: 1,
    definitionPages: 1,
    definitionSearch: "",
    activeOnly: true,
    selectedDefinition: null,
    selectedStatusSymbol: "",
    statusSymbols: [],
    definitionRequest: 0,
    statusRequest: 0,
    activeLoads: 0,
  };
  const $ = (selector) => document.querySelector(selector);
  const escape = (value) => String(value == null ? "--" : value).replace(
    /[&<>"']/g,
    (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character],
  );
  const number = (value, digits = 4) => value == null
    ? "--"
    : Number(value).toLocaleString("zh-CN", { maximumFractionDigits: digits });
  const compact = (value) => value == null
    ? "--"
    : new Intl.NumberFormat("zh-CN", { notation: "compact", maximumFractionDigits: 1 }).format(value);
  const positiveNumber = (value, digits = 0) => value == null || Number(value) <= 0
    ? "--"
    : number(value, digits);
  const dateOnly = (value) => value ? String(value).slice(0, 10) : "--";
  const timestamp = (value) => value
    ? new Date(value).toLocaleString("zh-CN", { timeZone: "UTC", hour12: false })
    : "--";
  const timeOnly = (value) => value
    ? new Date(value).toLocaleTimeString("zh-CN", {
        timeZone: "UTC", hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit",
      })
    : "--";
  const productInfo = (code) => state.summary?.products.find((item) => item.code === code);

  async function request(path) {
    const response = await fetch(`${API}${path}`);
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

  function params(values) {
    const search = new URLSearchParams();
    Object.entries(values).forEach(([key, value]) => {
      if (value !== "" && value != null) search.set(key, value);
    });
    return search.toString();
  }

  function showError(error) {
    const toast = $("#toast");
    toast.textContent = error?.message || String(error);
    toast.classList.remove("hidden");
    window.clearTimeout(showError.timer);
    showError.timer = window.setTimeout(() => toast.classList.add("hidden"), 5000);
  }

  function changeLoading(delta) {
    state.activeLoads = Math.max(0, state.activeLoads + delta);
    const loading = state.activeLoads > 0;
    $("#refresh-button").disabled = loading;
    $("#refresh-button").textContent = loading ? "读取中" : "刷新数据";
  }

  function renderProductNavigation() {
    const products = [{ code: "ALL", name: "全部六品种", kind: "" }, ...state.summary.products];
    $("#product-nav").innerHTML = products
      .filter((item) => state.view === "definition" || item.code !== "ALL")
      .map((item) => `
        <button class="product-item ${state.product === item.code ? "active" : ""}" type="button" data-product="${escape(item.code)}">
          <span><b class="product-code">${escape(item.code)}</b>${escape(item.name)}</span>
          <small>${escape(item.kind)}</small>
        </button>`).join("");
    document.querySelectorAll("[data-product]").forEach((button) => button.addEventListener("click", () => {
      const next = button.dataset.product;
      if (next === state.product) return;
      state.product = next;
      state.definitionPage = 1;
      state.selectedDefinition = null;
      if (state.view === "status") {
        const group = ["GC", "CL"].includes(next) ? "commodity" : "equity";
        state.statusDate = state.summary.status.latestCompleteDates[group];
        state.selectedStatusSymbol = "";
      }
      renderProductNavigation();
      syncDateInput();
      refreshCurrentView();
    }));
  }

  function syncDateInput() {
    const isDefinition = state.view === "definition";
    const input = $("#global-date");
    input.value = isDefinition ? state.definitionDate : state.statusDate;
    if (isDefinition) {
      input.min = dateOnly(state.summary.definition.start);
      input.max = state.summary.definition.latestDate;
    } else {
      const group = ["GC", "CL"].includes(state.product) ? "commodity" : "equity";
      const coverage = state.summary.status.coverage[group];
      input.min = dateOnly(coverage.ts_recv_min);
      input.max = dateOnly(coverage.ts_recv_max);
    }
    $("#date-label").textContent = isDefinition ? "历史时点（UTC）" : "接收日期（UTC）";
  }

  function setView(view) {
    state.view = view;
    if (view === "status" && state.product === "ALL") state.product = "ES";
    $("#definition-view").classList.toggle("hidden", view !== "definition");
    $("#status-view").classList.toggle("hidden", view !== "status");
    document.querySelectorAll("[data-view]").forEach((button) => {
      button.classList.toggle("active", button.dataset.view === view);
    });
    $("#page-eyebrow").textContent = view === "definition" ? "POINT-IN-TIME DEFINITION" : "SESSION STATUS · UTC";
    $("#page-title").textContent = view === "definition" ? "CME 合约定义" : "CME 交易状态";
    renderProductNavigation();
    syncDateInput();
    refreshCurrentView();
  }

  function definitionProductLabel() {
    if (state.product === "ALL") return "全部六品种";
    const info = productInfo(state.product);
    return `${state.product} · ${info?.name || state.product}`;
  }

  async function loadDefinitions() {
    const requestId = ++state.definitionRequest;
    changeLoading(1);
    $("#definition-body").innerHTML = '<tr class="loading-row"><td colspan="8">正在查询历史定义...</td></tr>';
    try {
      const query = {
        asOf: state.definitionDate,
        product: state.product === "ALL" ? "" : state.product,
        search: state.definitionSearch,
        activeOnly: state.activeOnly,
        page: state.definitionPage,
        pageSize: 50,
      };
      const [overview, result] = await Promise.all([
        request(`/definitions/overview?${params({ asOf: state.definitionDate })}`),
        request(`/definitions?${params(query)}`),
      ]);
      if (requestId !== state.definitionRequest || state.view !== "definition") return;
      state.definitionPages = result.pages;
      renderDefinitionMetrics(overview);
      renderDefinitions(result);
      if (state.selectedDefinition) {
        const current = result.records.find((record) => record.raw_symbol === state.selectedDefinition);
        if (current) showDefinitionDetail(current);
      }
    } catch (error) {
      if (requestId !== state.definitionRequest || state.view !== "definition") return;
      $("#definition-body").innerHTML = `<tr class="empty-row"><td colspan="8">${escape(error.message)}</td></tr>`;
      showError(error);
    } finally {
      changeLoading(-1);
    }
  }

  function renderDefinitionMetrics(overview) {
    $("#metric-active").textContent = number(overview.activeContracts, 0);
    $("#metric-symbols").textContent = number(state.summary.definition.uniqueSymbols, 0);
    $("#metric-definition-rows").textContent = compact(state.summary.definition.rows);
    $("#metric-definition-end").textContent = dateOnly(state.summary.definition.end);
  }

  function renderDefinitions(result) {
    $("#definition-result-summary").textContent = `${number(result.total, 0)} 个合约 · ${dateOnly(result.asOf)}`;
    $("#definition-scope").textContent = definitionProductLabel();
    $("#definition-page").textContent = `第 ${result.page} / ${result.pages} 页`;
    $("#definition-prev").disabled = result.page <= 1;
    $("#definition-next").disabled = result.page >= result.pages;
    if (!result.records.length) {
      $("#definition-body").innerHTML = '<tr class="empty-row"><td colspan="8">当前条件没有合约定义。</td></tr>';
      return;
    }
    $("#definition-body").innerHTML = result.records.map((record) => `
      <tr>
        <td><div class="cell-stack"><button class="symbol-button" type="button" data-definition-symbol="${escape(record.raw_symbol)}">${escape(record.raw_symbol)}</button><small>${escape(record.asset)}</small></div></td>
        <td><span class="venue-chip">${escape(record.exchange)}</span></td>
        <td><div class="cell-stack"><span>${escape(dateOnly(record.expiration))}</span><small>激活 ${escape(dateOnly(record.activation))}</small></div></td>
        <td>${number(record.min_price_increment, 8)}</td>
        <td><div class="cell-stack"><span>${number(record.unit_of_measure_qty, 4)}</span><small>${escape(record.unit_of_measure)}</small></div></td>
        <td>${record.currency === "USD" ? "$" : ""}${number(record.tick_value, 4)}</td>
        <td><div class="cell-stack"><span>${number(record.market_depth, 0)} 档</span><small>隐含 ${number(record.market_depth_implied, 0)}</small></div></td>
        <td>${escape(record.match_algorithm_label)}</td>
      </tr>`).join("");
    document.querySelectorAll("[data-definition-symbol]").forEach((button) => button.addEventListener("click", () => {
      const record = result.records.find((item) => item.raw_symbol === button.dataset.definitionSymbol);
      if (record) showDefinitionDetail(record);
    }));
  }

  function detailItem(label, value, wide = false) {
    return `<div class="${wide ? "detail-wide" : ""}"><dt>${escape(label)}</dt><dd>${escape(value)}</dd></div>`;
  }

  async function showDefinitionDetail(record) {
    state.selectedDefinition = record.raw_symbol;
    const info = productInfo(record.asset);
    $("#definition-detail").innerHTML = `
      <header class="detail-header">
        <div class="detail-header-top"><strong class="detail-symbol">${escape(record.raw_symbol)}</strong><span class="venue-chip">${escape(record.exchange)}</span></div>
        <p class="detail-product">${escape(info?.name || record.asset)} · ${escape(record.security_type)}</p>
      </header>
      <dl class="detail-grid">
        ${detailItem("Instrument ID", record.instrument_id)}
        ${detailItem("原生 ID", record.raw_instrument_id)}
        ${detailItem("最小跳动", number(record.min_price_increment, 8))}
        ${detailItem("每跳价值", `${record.currency === "USD" ? "$" : ""}${number(record.tick_value, 4)}`)}
        ${detailItem("合约规模", `${number(record.unit_of_measure_qty, 4)} ${record.unit_of_measure || ""}`)}
        ${detailItem("交易所增量金额", number(record.min_price_increment_amount, 8))}
        ${detailItem("最小交易量", number(record.min_trade_vol, 0))}
        ${detailItem("最大交易量", number(record.max_trade_vol, 0))}
        ${detailItem("最小下单量", positiveNumber(record.min_lot_size, 0))}
        ${detailItem("大宗最小量", positiveNumber(record.min_lot_size_block, 0))}
        ${detailItem("盘口 / 隐含深度", `${number(record.market_depth, 0)} / ${number(record.market_depth_implied, 0)}`)}
        ${detailItem("撮合算法", record.match_algorithm_label)}
        ${detailItem("当日价格上限", number(record.high_limit_price, 8))}
        ${detailItem("当日价格下限", number(record.low_limit_price, 8))}
        ${detailItem("最大价格变动", number(record.max_price_variation, 8))}
        ${detailItem("市场分段 ID", record.market_segment_id)}
        ${detailItem("激活时间", timestamp(record.activation), true)}
        ${detailItem("最后交易时间", timestamp(record.expiration), true)}
        ${detailItem("定义观测时间", timestamp(record.ts_recv), true)}
      </dl>
      <section class="detail-history"><h3>规格变更记录</h3><div class="history-list" id="detail-history-list"><span class="detail-product">正在读取...</span></div></section>`;
    try {
      const history = await request(`/definitions/${encodeURIComponent(record.raw_symbol)}?${params({ asOf: state.definitionDate })}`);
      if (state.selectedDefinition !== record.raw_symbol) return;
      const meaningful = history.changes.slice().reverse().slice(0, 8);
      $("#detail-history-list").innerHTML = meaningful.map((change) => `
        <div class="history-item">
          <strong>${escape(timestamp(change.ts_recv))} · ${escape(change.security_update_action)}</strong>
          <span>跳动 ${number(change.min_price_increment, 8)} · 规模 ${number(change.unit_of_measure_qty, 4)} · 到期 ${escape(dateOnly(change.expiration))}</span>
        </div>`).join("") || '<span class="detail-product">没有规格变更。</span>';
    } catch (error) {
      if (state.selectedDefinition === record.raw_symbol) {
        $("#detail-history-list").textContent = error.message;
      }
    }
  }

  function statusCoverage() {
    const key = ["GC", "CL"].includes(state.product) ? "commodity" : "equity";
    const coverage = state.summary.status.coverage[key];
    const products = coverage.products_kept.join(" / ");
    $("#status-coverage").innerHTML = `
      <span><strong>${escape(products)}</strong> 状态数据覆盖 ${escape(dateOnly(coverage.ts_recv_min))} 至 ${escape(dateOnly(coverage.ts_recv_max))}</span>
      <span>查询日期按 UTC 接收时间划分</span>`;
  }

  async function loadStatus() {
    const requestId = ++state.statusRequest;
    changeLoading(1);
    statusCoverage();
    $("#status-timeline").innerHTML = '<div class="timeline-empty">正在查询状态事件...</div>';
    $("#status-body").innerHTML = '<tr class="loading-row"><td colspan="7">正在查询状态事件...</td></tr>';
    try {
      const symbolResult = await request(`/status/symbols?${params({ product: state.product, date: state.statusDate })}`);
      if (requestId !== state.statusRequest || state.view !== "status") return;
      state.statusSymbols = symbolResult.records;
      if (!state.statusSymbols.some((item) => item.symbol === state.selectedStatusSymbol)) {
        state.selectedStatusSymbol = state.statusSymbols[0]?.symbol || "";
      }
      renderStatusSymbols();
      if (!state.selectedStatusSymbol) {
        renderEmptyStatus();
        return;
      }
      const result = await request(`/status?${params({
        product: state.product,
        date: state.statusDate,
        symbol: state.selectedStatusSymbol,
        pageSize: 1000,
      })}`);
      if (requestId !== state.statusRequest || state.view !== "status") return;
      renderStatus(result);
    } catch (error) {
      if (requestId !== state.statusRequest || state.view !== "status") return;
      $("#status-timeline").innerHTML = `<div class="timeline-empty">${escape(error.message)}</div>`;
      $("#status-body").innerHTML = `<tr class="empty-row"><td colspan="7">${escape(error.message)}</td></tr>`;
      showError(error);
    } finally {
      changeLoading(-1);
    }
  }

  function renderEmptyStatus() {
    $("#metric-events").textContent = "0";
    $("#metric-event-symbols").textContent = "0";
    $("#metric-exceptions").textContent = "0";
    $("#metric-status-symbol").textContent = "--";
    $("#status-result-summary").textContent = `0 条 · ${state.statusDate}`;
    $("#timeline-range").textContent = "无事件";
    $("#status-timeline").innerHTML = '<div class="timeline-empty">当前日期没有有效合约状态。</div>';
    $("#status-body").innerHTML = '<tr class="empty-row"><td colspan="7">当前日期没有有效合约状态。</td></tr>';
  }

  function renderStatusSymbols() {
    const select = $("#status-symbol");
    if (!state.statusSymbols.length) {
      select.innerHTML = '<option value="">当日无合约状态</option>';
      select.disabled = true;
      return;
    }
    select.disabled = false;
    select.innerHTML = state.statusSymbols.map((item) => `
      <option value="${escape(item.symbol)}" ${item.symbol === state.selectedStatusSymbol ? "selected" : ""}>
        ${escape(item.symbol)} · ${number(item.events, 0)} 条 · 到期 ${escape(dateOnly(item.expiration))}
      </option>`).join("");
  }

  function statusClass(record) {
    if (record.is_trading === "Y") return "trading";
    if (record.is_trading === "N") return "closed";
    return "unknown";
  }

  function actionClass(record) {
    if (["HALT", "PAUSE", "SUSPEND"].includes(record.action_name)) return "caution";
    return statusClass(record);
  }

  function triState(value) {
    if (value === "Y") return '<span class="tri-state yes">是</span>';
    if (value === "N") return '<span class="tri-state no">否</span>';
    return '<span class="tri-state unknown">未定义</span>';
  }

  function renderStatus(result) {
    const summary = result.summary;
    $("#metric-events").textContent = number(summary.events, 0);
    $("#metric-event-symbols").textContent = number(summary.symbols, 0);
    $("#metric-exceptions").textContent = number(summary.exceptional_events, 0);
    $("#metric-status-symbol").textContent = result.symbol || "--";
    $("#status-result-summary").textContent = `${number(result.total, 0)} 条 · ${escape(result.date)}`;
    $("#timeline-range").textContent = result.records.length
      ? `${timeOnly(result.records[0].ts_recv)} - ${timeOnly(result.records[result.records.length - 1].ts_recv)}`
      : "无事件";
    if (!result.records.length) {
      $("#status-timeline").innerHTML = '<div class="timeline-empty">当前日期没有状态事件。</div>';
      $("#status-body").innerHTML = '<tr class="empty-row"><td colspan="7">当前日期没有状态事件。</td></tr>';
      return;
    }
    $("#status-timeline").innerHTML = result.records.map((record) => `
      <article class="timeline-event ${statusClass(record)}">
        <time>${escape(timeOnly(record.ts_recv))}</time>
        <strong>${escape(record.action_label)}</strong>
        <small>${escape(record.reason_label)}</small>
        ${record.trading_event_name !== "NONE" ? `<small>${escape(record.trading_event_label)}</small>` : ""}
        ${record.is_midnight_snapshot ? '<small class="snapshot-label">UTC 日初快照</small>' : ""}
      </article>`).join("");
    $("#status-body").innerHTML = result.records.map((record) => `
      <tr>
        <td><div class="cell-stack"><span>${escape(timeOnly(record.ts_recv))}</span><small>${escape(record.is_midnight_snapshot ? "日初快照" : timestamp(record.ts_event))}</small></div></td>
        <td><span class="status-chip ${actionClass(record)}">${escape(record.action_label)}</span></td>
        <td><div class="cell-stack"><span>${escape(record.reason_label)}</span><small>${escape(record.reason_name)}</small></div></td>
        <td>${escape(record.trading_event_label)}</td>
        <td>${triState(record.is_trading)}</td>
        <td>${triState(record.is_quoting)}</td>
        <td>${escape(record.instrument_id)}</td>
      </tr>`).join("");
  }

  function refreshCurrentView() {
    return state.view === "definition" ? loadDefinitions() : loadStatus();
  }

  function bindEvents() {
    document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => setView(button.dataset.view)));
    $("#refresh-button").addEventListener("click", refreshCurrentView);
    $("#global-date").addEventListener("change", (event) => {
      if (!event.target.value) return;
      if (state.view === "definition") {
        state.definitionDate = event.target.value;
        state.definitionPage = 1;
        state.selectedDefinition = null;
      } else {
        state.statusDate = event.target.value;
        state.selectedStatusSymbol = "";
      }
      refreshCurrentView();
    });
    $("#active-only").addEventListener("change", (event) => {
      state.activeOnly = event.target.checked;
      state.definitionPage = 1;
      loadDefinitions();
    });
    $("#definition-search").addEventListener("input", (event) => {
      state.definitionSearch = event.target.value.trim();
      state.definitionPage = 1;
      window.clearTimeout(bindEvents.searchTimer);
      bindEvents.searchTimer = window.setTimeout(loadDefinitions, 250);
    });
    $("#definition-prev").addEventListener("click", () => {
      if (state.definitionPage > 1) {
        state.definitionPage -= 1;
        loadDefinitions();
      }
    });
    $("#definition-next").addEventListener("click", () => {
      if (state.definitionPage < state.definitionPages) {
        state.definitionPage += 1;
        loadDefinitions();
      }
    });
    $("#status-symbol").addEventListener("change", (event) => {
      state.selectedStatusSymbol = event.target.value;
      loadStatus();
    });
  }

  async function initialize() {
    bindEvents();
    try {
      state.summary = await request("/summary");
      state.definitionDate = state.summary.definition.latestDate;
      state.statusDate = state.summary.status.latestCompleteDates.equity;
      $("#definition-nav-count").textContent = number(state.summary.definition.uniqueSymbols, 0);
      $("#status-nav-count").textContent = compact(state.summary.status.rows);
      $("#connection-state").textContent = "本地数据就绪";
      $("#connection-dot").classList.add("online");
      renderProductNavigation();
      syncDateInput();
      await loadDefinitions();
    } catch (error) {
      $("#connection-state").textContent = "数据读取失败";
      $("#connection-dot").classList.add("error");
      showError(error);
    }
  }

  initialize();
})();
