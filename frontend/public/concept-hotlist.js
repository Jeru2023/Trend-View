console.info("Concept hotlist module v20270437");

const translations = getTranslations("conceptHotlist");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const HOT_LIMIT = 5;
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
const CHART_LIMIT = 120;

const elements = {
  refreshButton: document.getElementById("concept-hotlist-refresh"),
  status: document.getElementById("concept-hotlist-status"),
  generatedAt: document.getElementById("concept-hotlist-generated-at"),
  symbols: document.getElementById("concept-hotlist-symbols"),
  tabs: document.getElementById("concept-hotlist-tabs"),
  chart: document.getElementById("concept-hotlist-chart"),
  detail: document.getElementById("concept-hotlist-detail"),
  hotDate: document.getElementById("concept-hotlist-hot-date"),
  langButtons: document.querySelectorAll(".lang-btn"),
};

const state = {
  lang: null,
  snapshot: null,
  loading: false,
  topConcepts: [],
  activeConcept: null,
  chartInstance: null,
  chartRequestToken: null,
  chartConcept: null,
  chartRows: [],
  chartResizeAttached: false,
};

let echartsLoader = null;

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) return stored;
  } catch (error) {
    /* ignore */
  }
  const htmlLang = document.documentElement.getAttribute("data-pref-lang") || document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) return htmlLang;
  const navigatorLang = (navigator.language || "").toLowerCase();
  return translations[navigatorLang] ? navigatorLang : "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* ignore */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

function getDict() {
  return translations[state.lang] || translations.zh || translations.en;
}

function setStatus(message, tone) {
  const node = elements.status;
  if (!node) return;
  if (!message) {
    node.textContent = "";
    node.removeAttribute("data-tone");
    return;
  }
  node.textContent = message;
  if (tone) {
    node.setAttribute("data-tone", tone);
  } else {
    node.removeAttribute("data-tone");
  }
}

function setLoading(isLoading) {
  state.loading = isLoading;
  const button = elements.refreshButton;
  const dict = getDict();
  if (!button) return;
  const label = button.querySelector(".btn__label");
  if (isLoading) {
    button.disabled = true;
    button.dataset.loading = "1";
    if (label) label.textContent = dict.refreshing || "Refreshing...";
  } else {
    button.disabled = false;
    delete button.dataset.loading;
    if (label) label.textContent = dict.refreshButton || "Refresh";
  }
}

function formatDateTime(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return numeric.toFixed(digits);
}

function formatPercent(value, digits = 1) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return `${numeric.toFixed(digits)}%`;
}

function formatMoney(value) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const absValue = Math.abs(numeric);
  if (absValue >= 1e8) {
    const unit = state.lang === "zh" ? "亿" : "B";
    return `${(numeric / 1e8).toFixed(2)}${unit}`;
  }
  if (absValue >= 1e4) {
    const unit = state.lang === "zh" ? "万" : "K";
    return `${(numeric / 1e4).toFixed(1)}${unit}`;
  }
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return numeric.toLocaleString(locale, { maximumFractionDigits: 2 });
}

function formatDateOnly(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return date.toLocaleDateString(locale, { year: "numeric", month: "short", day: "numeric" });
}

function formatLeading(stock, change) {
  if (!stock) return "--";
  const changeText = formatPercent(change);
  return changeText === "--" ? stock : `${stock} (${changeText})`;
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatCompactNumber(value) {
  const numeric = toNumber(value);
  if (numeric === null) return "";
  const abs = Math.abs(numeric);
  if (abs >= 1e8) {
    const unit = state.lang === "zh" ? "亿" : "B";
    return `${(numeric / 1e8).toFixed(1)}${unit}`;
  }
  if (abs >= 1e6) {
    return `${(numeric / 1e6).toFixed(1)}M`;
  }
  if (abs >= 1e3) {
    return `${(numeric / 1e3).toFixed(0)}K`;
  }
  return numeric.toFixed(0);
}

function formatChartDate(value) {
  if (!value) return "--";
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  const text = String(value).trim();
  if (/^\d{8}$/.test(text)) {
    return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  }
  if (/^\d{4}-\d{2}-\d{2}/.test(text)) {
    return text.slice(0, 10);
  }
  return text.slice(0, 10) || "--";
}

function clearContainer(node, message) {
  if (!node) return;
  node.innerHTML = "";
  if (message) {
    const placeholder = document.createElement("div");
    placeholder.className = "flow-card__empty";
    placeholder.textContent = message;
    node.appendChild(placeholder);
  }
}

function setHotPlaceholder(node, message) {
  if (!node) return;
  node.innerHTML = "";
  if (!message) return;
  const placeholder = document.createElement("div");
  placeholder.className = "hotlist-hot__placeholder";
  placeholder.textContent = message;
  node.appendChild(placeholder);
}

function handleChartResize() {
  if (state.chartInstance) {
    state.chartInstance.resize();
  }
}

function loadEcharts() {
  if (window.echarts) {
    return Promise.resolve(window.echarts);
  }
  if (echartsLoader) {
    return echartsLoader;
  }
  echartsLoader = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = ECHARTS_CDN;
    script.async = true;
    script.onload = () => resolve(window.echarts);
    script.onerror = (error) => reject(error);
    document.head.appendChild(script);
  }).finally(() => {
    echartsLoader = null;
  });
  return echartsLoader;
}

function showChartPlaceholder(message) {
  const container = elements.chart;
  if (!container) return;
  if (state.chartInstance) {
    state.chartInstance.dispose();
    state.chartInstance = null;
  }
  delete container.dataset.loading;
  container.innerHTML = "";
  const dict = getDict();
  const key = state.lang === "zh" ? "Zh" : "En";
  const fallback = container.dataset[`empty${key}`];
  const placeholder = document.createElement("div");
  placeholder.className = "hotlist-hot__placeholder";
  placeholder.textContent = message || fallback || dict.hotChartEmpty || "No chart data.";
  container.appendChild(placeholder);
}

async function renderConceptChart(rows, conceptName) {
  const container = elements.chart;
  if (!container) return;
  if (!rows || !rows.length) {
    showChartPlaceholder(getDict().hotChartEmpty || "No chart data.");
    return;
  }

  if (!state.chartInstance) {
    container.innerHTML = "";
    const echarts = await loadEcharts();
    state.chartInstance = echarts.init(container);
    if (!state.chartResizeAttached) {
      window.addEventListener("resize", handleChartResize);
      state.chartResizeAttached = true;
    }
  } else {
    state.chartInstance.clear();
  }

  const dict = getDict();
  const sorted = [...rows].sort((a, b) => {
    const da = new Date(a.tradeDate || a.trade_date);
    const db = new Date(b.tradeDate || b.trade_date);
    return da - db;
  });
  const categories = sorted.map((item) => formatChartDate(item.tradeDate || item.trade_date));
  const candles = sorted.map((item) => {
    const open = toNumber(item.open);
    const close = toNumber(item.close);
    const low = toNumber(item.low);
    const high = toNumber(item.high);
    const reference = close ?? open ?? toNumber(item.preClose || item.pre_close) ?? 0;
    const candleLow = low ?? Math.min(open ?? reference, close ?? reference, reference);
    const candleHigh = high ?? Math.max(open ?? reference, close ?? reference, reference);
    return [open ?? reference, close ?? reference, candleLow, candleHigh];
  });
  const volumes = sorted.map((item) => toNumber(item.vol) ?? 0);

  const option = {
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
    },
    axisPointer: {
      link: [{ xAxisIndex: [0, 1] }],
    },
    dataZoom: [
      { type: "inside", xAxisIndex: [0, 1], start: 70, end: 100 },
      { type: "slider", xAxisIndex: [0, 1], top: 285, start: 70, end: 100 },
    ],
    grid: [
      { left: 50, right: 16, top: 10, height: 190 },
      { left: 50, right: 16, top: 220, height: 60 },
    ],
    xAxis: [
      {
        type: "category",
        data: categories,
        boundaryGap: false,
        axisLine: { onZero: false },
        splitLine: { show: false },
        min: "dataMin",
        max: "dataMax",
      },
      {
        type: "category",
        gridIndex: 1,
        data: categories,
        boundaryGap: false,
        axisLine: { onZero: false },
        axisTick: { show: false },
        splitLine: { show: false },
        min: "dataMin",
        max: "dataMax",
      },
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: true },
        axisLabel: {
          formatter: (value) => formatNumber(value, 0),
        },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: {
          formatter: (value) => formatCompactNumber(value) || "0",
        },
      },
    ],
    series: [
      {
        name: conceptName || dict.seriesLabel || "Concept",
        type: "candlestick",
        data: candles,
        itemStyle: {
          color: "#ef5350",
          color0: "#26a69a",
          borderColor: "#ef5350",
          borderColor0: "#26a69a",
        },
      },
      {
        name: dict.volumeLabel || "Volume",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        itemStyle: {
          color: "#93c5fd",
        },
      },
    ],
  };

  state.chartInstance.setOption(option, true);
}

async function loadConceptChart(conceptName) {
  if (!elements.chart) return;
  const dict = getDict();
  if (!conceptName) {
    state.chartRows = [];
    state.chartConcept = null;
    showChartPlaceholder(dict.hotChartEmpty || "No chart data.");
    return;
  }

  const requestToken = Symbol("conceptChart");
  state.chartRequestToken = requestToken;
  elements.chart.dataset.loading = "1";
  try {
    const response = await fetch(
      `${API_BASE}/market/concept-index-history?concept=${encodeURIComponent(conceptName)}&limit=${CHART_LIMIT}`
    );
    if (!response.ok) {
      throw new Error(`Failed to load concept index history (${response.status})`);
    }
    const payload = await response.json();
    if (state.chartRequestToken !== requestToken) {
      return;
    }
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!rows.length) {
      state.chartRows = [];
      state.chartConcept = null;
      showChartPlaceholder(dict.hotChartEmpty || "No chart data.");
      return;
    }
    state.chartRows = rows;
    state.chartConcept = conceptName;
    await renderConceptChart(rows, conceptName);
  } catch (error) {
    console.error(`Concept chart fetch failed for ${conceptName}`, error);
    if (state.chartRequestToken === requestToken) {
      state.chartRows = [];
      state.chartConcept = null;
      showChartPlaceholder(dict.hotChartError || dict.statusError || "Failed to load chart.");
    }
  } finally {
    if (state.chartRequestToken === requestToken) {
      delete elements.chart.dataset.loading;
      state.chartRequestToken = null;
    }
  }
}

function renderSymbols(snapshot) {
  const container = elements.symbols;
  if (!container) return;
  container.innerHTML = "";
  const dict = getDict();
  const symbols = snapshot?.symbols || [];
  if (!symbols.length) {
    const placeholder = document.createElement("span");
    placeholder.textContent = dict.emptyHotlist || "No data available.";
    container.appendChild(placeholder);
    return;
  }
  symbols.forEach((item) => {
    const chip = document.createElement("span");
    chip.textContent = `${item.symbol} · ${dict.weightLabel || "weight"} ${formatNumber(item.weight, 2)}`;
    container.appendChild(chip);
  });
}

function buildStageTable(stages) {
  const dict = getDict();
  const table = document.createElement("table");
  table.className = "hotlist-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  [
    dict.tableHeaderSymbol || "Period",
    dict.tableHeaderRank || "Rank",
    dict.tableHeaderNet || "Net inflow",
    dict.tableHeaderInflow || "Inflow",
    dict.tableHeaderOutflow || "Outflow",
    dict.tableHeaderPriceChange || "Price %",
    dict.tableHeaderStageChange || "Stage %",
    dict.tableHeaderLeading || "Leader",
    dict.tableHeaderUpdated || "Updated",
  ].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  stages.forEach((stage) => {
    const row = document.createElement("tr");
    const cells = [
      stage.symbol,
      stage.rank ?? "--",
      formatMoney(stage.netAmount),
      formatMoney(stage.inflow),
      formatMoney(stage.outflow),
      formatPercent(stage.priceChangePercent, 1),
      formatPercent(stage.stageChangePercent, 1),
      formatLeading(stage.leadingStock, stage.leadingStockChangePercent),
      stage.updatedAt ? formatDateTime(stage.updatedAt) : "--",
    ];
    cells.forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value === undefined ? "--" : value;
      row.appendChild(td);
    });
    tbody.appendChild(row);
  });
  table.appendChild(tbody);
  return table;
}

function renderMeta(snapshot) {
  if (elements.generatedAt) {
    elements.generatedAt.textContent = snapshot?.generatedAt ? formatDateTime(snapshot.generatedAt) : "--";
  }
}

function deriveTopConcepts(snapshot) {
  const concepts = Array.isArray(snapshot?.concepts) ? snapshot.concepts : [];
  return concepts
    .slice()
    .sort((a, b) => {
      const scoreA = a.score ?? a.fundFlow?.score ?? -Infinity;
      const scoreB = b.score ?? b.fundFlow?.score ?? -Infinity;
      if (scoreA === scoreB) {
        const netA = a.totalNetAmount ?? a.fundFlow?.totalNetAmount ?? 0;
        const netB = b.totalNetAmount ?? b.fundFlow?.totalNetAmount ?? 0;
        return netB - netA;
      }
      return scoreB - scoreA;
    })
    .slice(0, HOT_LIMIT);
}

function getActiveConcept() {
  return state.topConcepts.find((item) => item.name === state.activeConcept) || null;
}

function renderConceptTabs(concepts) {
  const container = elements.tabs;
  if (!container) return;
  const dict = getDict();
  container.innerHTML = "";
  if (!Array.isArray(concepts) || !concepts.length) {
    const message = dict.hotListEmpty || container.dataset[`empty${state.lang === "zh" ? "Zh" : "En"}`];
    setHotPlaceholder(container, message || "No hot concepts.");
    return;
  }

  concepts.forEach((concept, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `hotlist-hot__tab${concept.name === state.activeConcept ? " hotlist-hot__tab--active" : ""}`;
    button.dataset.concept = concept.name || "";

    const rank = document.createElement("span");
    rank.className = "hotlist-hot__tab-rank";
    rank.textContent = `TOP ${index + 1}`;
    button.appendChild(rank);

    const title = document.createElement("h4");
    title.className = "hotlist-hot__tab-title";
    title.textContent = concept.name || "--";
    button.appendChild(title);

    const metas = document.createElement("div");
    metas.className = "hotlist-hot__tab-metas";
    metas.innerHTML = `
      <span>${dict.scoreLabel || "Score"} ${formatNumber(concept.score, 3)}</span>
      <span>${dict.totalNetLabel || "Net"} ${formatMoney(concept.totalNetAmount)}</span>
    `;
    button.appendChild(metas);

    button.addEventListener("click", () => {
      if (concept.name) {
        setActiveConcept(concept.name);
      }
    });
    container.appendChild(button);
  });
}

function renderConceptDetail(concept) {
  const container = elements.detail;
  if (!container) return;
  const dict = getDict();
  container.innerHTML = "";
  if (!concept) {
    setHotPlaceholder(container, dict.noConceptData || "No data available.");
    return;
  }

  const card = document.createElement("article");
  card.className = "flow-card";

  const header = document.createElement("header");
  header.className = "flow-card__header";
  const title = document.createElement("h3");
  title.className = "flow-card__title";
  title.textContent = concept.name || "--";
  header.appendChild(title);
  card.appendChild(header);

  const summary = document.createElement("div");
  summary.className = "flow-card__summary";
  summary.innerHTML = `
    <div class="flow-card__summary-item">
      <span>${dict.scoreLabel || "Score"}</span>
      <strong>${formatNumber(concept.score, 3)}</strong>
    </div>
    <div class="flow-card__summary-item">
      <span>${dict.totalNetLabel || "Total net"}</span>
      <strong>${formatMoney(concept.totalNetAmount)}</strong>
    </div>
    <div class="flow-card__summary-item">
      <span>${dict.bestRankLabel || "Best rank"}</span>
      <strong>${concept.bestRank ?? "--"}</strong>
    </div>
  `;
  card.appendChild(summary);

  const stats = document.createElement("div");
  stats.className = "flow-card__stats";
  stats.innerHTML = `
    <div class="flow-card__stat">
      <span>${dict.totalInflowLabel || "Inflow"}</span>
      <strong>${formatMoney(concept.totalInflow)}</strong>
    </div>
    <div class="flow-card__stat">
      <span>${dict.totalOutflowLabel || "Outflow"}</span>
      <strong>${formatMoney(concept.totalOutflow)}</strong>
    </div>
    <div class="flow-card__stat">
      <span>${dict.bestStageLabel || "Best period"}</span>
      <strong>${concept.bestSymbol || "--"}</strong>
    </div>
  `;
  card.appendChild(stats);

  if (Array.isArray(concept.stages) && concept.stages.length) {
    const table = buildStageTable(concept.stages);
    if (table) {
      const wrapper = document.createElement("div");
      wrapper.className = "flow-card__table-wrapper";
      wrapper.appendChild(table);
      card.appendChild(wrapper);
    }
  }

  container.appendChild(card);
}

function setActiveConcept(conceptName) {
  if (!conceptName) return;
  state.activeConcept = conceptName;
  renderConceptTabs(state.topConcepts);
  const concept = getActiveConcept();
  renderConceptDetail(concept);
  if (concept?.name) {
    loadConceptChart(concept.name);
  } else {
    showChartPlaceholder(getDict().hotChartEmpty || "No chart data.");
  }
}

function renderTopConcepts(snapshot) {
  const dict = getDict();
  const top = deriveTopConcepts(snapshot);
  state.topConcepts = top;
  if (elements.hotDate) {
    elements.hotDate.textContent = snapshot?.generatedAt ? formatDateOnly(snapshot.generatedAt) : "--";
  }
  if (!top.length) {
    setHotPlaceholder(elements.tabs, dict.hotListEmpty || "No hot concepts.");
    setHotPlaceholder(elements.detail, dict.noConceptData || "No data available.");
    showChartPlaceholder(dict.hotChartEmpty || "No chart data.");
    state.activeConcept = null;
    state.chartConcept = null;
    state.chartRows = [];
    return;
  }
  if (!top.some((item) => item.name === state.activeConcept)) {
    state.activeConcept = top[0].name || null;
  }
  renderConceptTabs(top);
  const active = getActiveConcept();
  renderConceptDetail(active);
  if (active?.name) {
    loadConceptChart(active.name);
  } else {
    showChartPlaceholder(dict.hotChartEmpty || "No chart data.");
  }
}

function renderAll() {
  renderMeta(state.snapshot);
  renderSymbols(state.snapshot);
  renderTopConcepts(state.snapshot);
}

async function fetchHotlist() {
  setLoading(true);
  setStatus(getDict().statusLoading || "Loading latest rankings…", "info");
  try {
    const response = await fetch(`${API_BASE}/fund-flow/sector-hotlist`);
    if (!response.ok) throw new Error(`Failed to load sector hotlist: ${response.status}`);
    const snapshot = await response.json();
    state.snapshot = snapshot;
    renderAll();
    setStatus(`${getDict().statusLoaded || "Ranking updated."} ${formatDateTime(snapshot.generatedAt)}`);
  } catch (error) {
    console.error(error);
    setStatus(getDict().statusError || "Failed to load ranking, please retry.", "error");
  } finally {
    setLoading(false);
  }
}

function handleLanguageSwitch(event) {
  const button = event.currentTarget;
  const lang = button?.dataset?.lang;
  if (!lang || lang === state.lang || !translations[lang]) return;
  state.lang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((node) => {
    if (node.dataset.lang === lang) {
      node.classList.add("lang-btn--active");
    } else {
      node.classList.remove("lang-btn--active");
    }
  });
  renderAll();
}

function initLanguage() {
  state.lang = getInitialLanguage();
  elements.langButtons.forEach((button) => {
    if (button.dataset.lang === state.lang) {
      button.classList.add("lang-btn--active");
    }
    button.addEventListener("click", handleLanguageSwitch);
  });
  persistLanguage(state.lang);
}

function init() {
  initLanguage();
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => {
      if (!state.loading) fetchHotlist();
    });
  }
  fetchHotlist();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
