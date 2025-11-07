const translations = getTranslations("conceptMarket");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const LANG_STORAGE_KEY = "trend-view-lang";
const DEFAULT_SYMBOL = "即时";
const DEFAULT_LIMIT = 90;
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  chartContainer: document.getElementById("concept-market-chart"),
  chartUpdated: document.getElementById("concept-market-chart-updated"),
  conceptSelect: document.getElementById("concept-market-select"),
  conceptList: document.getElementById("concept-market-list"),
};

const state = {
  lang: getInitialLanguage(),
  hotlist: [],
  currentConcept: null,
  chartInstance: null,
  loadingChart: false,
  chartRows: [],
  chartRequestToken: null,
};

let echartsLoader = null;

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    /* no-op */
  }
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  return "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* no-op */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

function getDict() {
  return translations[state.lang] || translations.zh || translations.en;
}

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(numeric);
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric.toFixed(digits)}%`;
}

function formatTenThousand(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return formatNumber(numeric / 10000, { maximumFractionDigits: 1 });
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric;
}

function applyTranslations() {
  const dict = getDict();
  document.title = dict.title || document.title;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === state.lang);
  });
}

window.applyTranslations = applyTranslations;

elements.langButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    const lang = btn.dataset.lang;
    if (lang && translations[lang]) {
      state.lang = lang;
      persistLanguage(lang);
      applyTranslations();
      renderConceptList();
      if (state.chartRows) {
        renderChart(state.chartRows);
      }
    }
  });
});

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

async function loadHotConcepts() {
  const listNode = elements.conceptList;
  if (listNode) {
    listNode.dataset.loading = "1";
  }
  try {
    const response = await fetch(
      `${API_BASE}/fund-flow/concept?symbol=${encodeURIComponent(DEFAULT_SYMBOL)}&limit=30`
    );
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    state.hotlist = Array.isArray(data.items) ? data.items : [];
    renderConceptList();
    renderConceptOptions();
    if (state.hotlist.length) {
      if (!state.currentConcept) {
        setCurrentConcept(state.hotlist[0].concept);
      } else {
        setCurrentConcept(state.currentConcept);
      }
    } else {
      showChartPlaceholder(getDict().listEmpty || "No concept snapshot.");
      elements.chartUpdated.textContent = "--";
    }
  } catch (error) {
    console.error("Failed to load concept snapshot", error);
    state.hotlist = [];
    renderConceptList(true);
  } finally {
    if (listNode) {
      delete listNode.dataset.loading;
    }
  }
}

function renderConceptOptions() {
  const select = elements.conceptSelect;
  if (!select) return;
  const dict = getDict();
  select.innerHTML = "";
  if (!state.hotlist.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = dict.listEmpty || "No data";
    select.appendChild(option);
    select.disabled = true;
    return;
  }
  select.disabled = false;
  state.hotlist.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.concept;
    option.textContent = item.concept;
    select.appendChild(option);
  });
  select.value = state.currentConcept || state.hotlist[0].concept;
}

function renderConceptList(error = false) {
  const container = elements.conceptList;
  if (!container) return;
  container.innerHTML = "";
  const dict = getDict();
  if (error) {
    const placeholder = document.createElement("div");
    placeholder.className = "empty-placeholder";
    placeholder.textContent = dict.fetchFailed || "Failed to load concept data.";
    container.appendChild(placeholder);
    return;
  }
  if (!state.hotlist.length) {
    const placeholder = document.createElement("div");
    placeholder.className = "empty-placeholder";
    placeholder.textContent = dict.listEmpty || "No concept snapshot.";
    container.appendChild(placeholder);
    return;
  }
  const table = document.createElement("table");
  table.className = "data-table concept-market__table";
  const thead = document.createElement("thead");
  thead.innerHTML = `
    <tr>
      <th>${dict.columnConcept || "Concept"}</th>
      <th>${dict.columnRank || "Rank"}</th>
      <th>${dict.columnNetAmount || "Net"}</th>
      <th>${dict.columnInflow || "Inflow"}</th>
      <th>${dict.columnOutflow || "Outflow"}</th>
      <th>${dict.columnChange || "Price %"}</th>
      <th>${dict.columnLeading || "Leading"}</th>
    </tr>
  `;
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  state.hotlist.forEach((item) => {
    const row = document.createElement("tr");
    row.dataset.concept = item.concept;
    row.innerHTML = `
      <td>${item.concept}</td>
      <td>${item.rank ?? "--"}</td>
      <td>${formatTenThousand(item.net_amount)}</td>
      <td>${formatTenThousand(item.inflow)}</td>
      <td>${formatTenThousand(item.outflow)}</td>
      <td>${formatPercent(item.price_change_percent)}</td>
      <td>${item.leading_stock || "--"}</td>
    `;
    row.addEventListener("click", () => {
      setCurrentConcept(item.concept);
    });
    tbody.appendChild(row);
  });
  table.appendChild(tbody);
  container.appendChild(table);
  highlightSelectedConceptRow(state.currentConcept);
}

function highlightSelectedConceptRow(concept) {
  if (!elements.conceptList) return;
  const rows = elements.conceptList.querySelectorAll("tbody tr");
  rows.forEach((row) => {
    row.classList.toggle("is-active", !!concept && row.dataset.concept === concept);
  });
}

function setCurrentConcept(concept) {
  if (!concept) return;
  state.currentConcept = concept;
  if (elements.conceptSelect) {
    elements.conceptSelect.value = concept;
  }
  highlightSelectedConceptRow(concept);
  loadConceptHistory(concept);
}

async function loadConceptHistory(concept) {
  if (!concept || !elements.chartContainer) {
    return;
  }
  state.loadingChart = true;
  elements.chartUpdated.textContent = "--";
  elements.chartContainer.classList.add("is-loading");
  const dict = getDict();
  const requestToken = Symbol("conceptHistory");
  state.chartRequestToken = requestToken;
  try {
    const response = await fetch(
      `${API_BASE}/market/concept-index-history?concept=${encodeURIComponent(concept)}&limit=${DEFAULT_LIMIT}`
    );
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    const rows = Array.isArray(data.rows) ? data.rows : [];
    if (state.chartRequestToken !== requestToken) {
      return;
    }
    state.chartRows = rows;
    if (!rows.length) {
      renderChart([]);
      elements.chartUpdated.textContent = "--";
      showChartPlaceholder(dict.noChartData || "No candlestick data available.");
      return;
    }
    const latest = rows[0]?.tradeDate || rows[0]?.trade_date;
    elements.chartUpdated.textContent = formatDate(latest);
    renderChart(rows);
  } catch (error) {
    console.error("Failed to load concept index history", error);
    if (state.chartRequestToken === requestToken) {
      showChartPlaceholder(dict.fetchFailed || "Failed to load concept data.");
      elements.chartUpdated.textContent = "--";
    }
  } finally {
    if (state.chartRequestToken === requestToken) {
      state.loadingChart = false;
      elements.chartContainer.classList.remove("is-loading");
    }
  }
}

function formatDate(value) {
  if (!value) return "--";
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{8}$/.test(trimmed)) {
      return `${trimmed.slice(0, 4)}-${trimmed.slice(4, 6)}-${trimmed.slice(6, 8)}`;
    }
    return trimmed.slice(0, 10);
  }
  const str = String(value);
  if (/^\d{8}$/.test(str)) {
    return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
  }
  return str.slice(0, 10);
}

function showChartPlaceholder(message) {
  if (!elements.chartContainer) return;
  elements.chartContainer.innerHTML = "";
  const placeholder = document.createElement("div");
  placeholder.className = "empty-placeholder";
  placeholder.textContent = message;
  elements.chartContainer.appendChild(placeholder);
  if (state.chartInstance) {
    state.chartInstance.dispose();
    state.chartInstance = null;
  }
}

async function renderChart(rows) {
  if (!elements.chartContainer) return;
  if (!rows || !rows.length) {
    showChartPlaceholder(getDict().noChartData || "No candlestick data available.");
    return;
  }
  elements.chartContainer.innerHTML = "";
  const echarts = await loadEcharts();
  if (!state.chartInstance) {
    state.chartInstance = echarts.init(elements.chartContainer);
    window.addEventListener("resize", () => {
      if (state.chartInstance) {
        state.chartInstance.resize();
      }
    });
  }

  const sorted = [...rows].sort((a, b) => {
    const da = new Date(a.tradeDate || a.trade_date);
    const db = new Date(b.tradeDate || b.trade_date);
    return da - db;
  });
  const categories = sorted.map((item) => formatDate(item.tradeDate || item.trade_date));
  const candles = sorted.map((item) => {
    const open = toNumber(item.open);
    const close = toNumber(item.close);
    const low = toNumber(item.low);
    const high = toNumber(item.high);
    const reference = close ?? open ?? toNumber(item.pre_close) ?? 0;
    const lowValue = low ?? Math.min(open ?? reference, close ?? reference, reference);
    const highValue = high ?? Math.max(open ?? reference, close ?? reference, reference);
    return [open ?? reference, close ?? reference, lowValue, highValue];
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
      {
        type: "inside",
        xAxisIndex: [0, 1],
        start: 60,
        end: 100,
      },
      {
        type: "slider",
        xAxisIndex: [0, 1],
        top: 360,
        start: 60,
        end: 100,
      },
    ],
    grid: [
      { left: 60, right: 20, top: 20, height: 220 },
      { left: 60, right: 20, top: 260, height: 80 },
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
        splitLine: { show: false },
        axisTick: { show: false },
        min: "dataMin",
        max: "dataMax",
      },
    ],
    yAxis: [
      {
        scale: true,
        splitArea: { show: true },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: {
          formatter: (value) => formatNumber(value, { notation: "compact", maximumFractionDigits: 1 }),
        },
      },
    ],
    series: [
      {
        name: state.currentConcept || "Concept",
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
        name: "Volume",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        itemStyle: {
          color: "#90caf9",
        },
      },
    ],
  };

  state.chartInstance.setOption(option, true);
}

if (elements.conceptSelect) {
  elements.conceptSelect.addEventListener("change", (event) => {
    const value = event.target.value;
    if (value) {
      setCurrentConcept(value);
    }
  });
}

function init() {
  applyTranslations();
  loadHotConcepts();
}

init();
