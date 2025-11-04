const translations = getTranslations("marketFundFlow");
const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";

let echartsLoader = null;
let chartInstance = null;
let resizeListenerBound = false;
let currentLang = getInitialLanguage();
let currentItems = [];
const MAX_CARD_ITEMS = 60;

const state = {
  startDate: "",
  endDate: "",
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  startDate: document.getElementById("market-fund-flow-start"),
  endDate: document.getElementById("market-fund-flow-end"),
  resetButton: document.getElementById("market-fund-flow-reset"),
  chartContainer: document.getElementById("market-fund-flow-chart"),
  chartEmpty: document.getElementById("market-fund-flow-chart-empty"),
  cardsContainer: document.getElementById("market-fund-flow-cards"),
};

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
  const htmlLang = document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) {
    return htmlLang;
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
  return translations[currentLang] || translations.en;
}

function ensureEchartsLoaded() {
  if (window.echarts) {
    return Promise.resolve();
  }
  if (echartsLoader) {
    return echartsLoader;
  }
  echartsLoader = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = ECHARTS_CDN;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => {
      echartsLoader = null;
      reject(new Error("Failed to load chart library"));
    };
    document.head.appendChild(script);
  });
  return echartsLoader;
}

function ensureChartInstance() {
  if (!window.echarts || !elements.chartContainer) {
    return null;
  }
  if (!chartInstance) {
    chartInstance = window.echarts.init(elements.chartContainer);
  }
  if (!resizeListenerBound) {
    window.addEventListener("resize", handleResize);
    resizeListenerBound = true;
  }
  return chartInstance;
}

function handleResize() {
  if (chartInstance) {
    chartInstance.resize();
  }
}

function disposeChart() {
  if (chartInstance) {
    chartInstance.dispose();
    chartInstance = null;
  }
  if (resizeListenerBound) {
    window.removeEventListener("resize", handleResize);
    resizeListenerBound = false;
  }
}

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(numeric);
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value) * 100;
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric.toFixed(digits)}%`;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString().slice(0, 10);
    }
  } catch (error) {
    /* no-op */
  }
  return String(value);
}

const CAMEL_CACHE = new Map();
function toCamel(key) {
  if (CAMEL_CACHE.has(key)) {
    return CAMEL_CACHE.get(key);
  }
  const camel = key.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
  CAMEL_CACHE.set(key, camel);
  return camel;
}

function getRecordValue(record, key) {
  if (!record) {
    return undefined;
  }
  if (Object.prototype.hasOwnProperty.call(record, key)) {
    return record[key];
  }
  const camelKey = toCamel(key);
  if (Object.prototype.hasOwnProperty.call(record, camelKey)) {
    return record[camelKey];
  }
  return undefined;
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.title = dict.title || document.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      el.textContent = value;
    }
  });

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  syncDateInputs();
  renderCards(currentItems);
  renderChart(currentItems);
}

function syncDateInputs() {
  if (elements.startDate) {
    elements.startDate.value = state.startDate || "";
  }
  if (elements.endDate) {
    elements.endDate.value = state.endDate || "";
  }
}

function validateDateRange() {
  if (state.startDate && state.endDate && state.startDate > state.endDate) {
    const tmp = state.startDate;
    state.startDate = state.endDate;
    state.endDate = tmp;
  }
}

function renderChart(items) {
  if (!elements.chartContainer || !elements.chartEmpty) {
    return;
  }
  if (!items || !items.length) {
    disposeChart();
    elements.chartContainer.classList.add("hidden");
    elements.chartEmpty.classList.remove("hidden");
    return;
  }

  ensureEchartsLoaded()
    .then(() => {
      const instance = ensureChartInstance();
      if (!instance) {
        return;
      }

      const sorted = [...items].sort(
        (a, b) =>
          new Date(getRecordValue(a, "trade_date") || getRecordValue(a, "tradeDate")) -
          new Date(getRecordValue(b, "trade_date") || getRecordValue(b, "tradeDate"))
      );

      const xAxis = sorted.map(
        (item) => getRecordValue(item, "trade_date") || getRecordValue(item, "tradeDate")
      );
      const seriesDefs = [
        {
          key: "main_net_inflow_amount",
          name: getDict().seriesMain,
        },
        {
          key: "large_order_net_inflow_amount",
          name: getDict().seriesLarge,
        },
        {
          key: "medium_order_net_inflow_amount",
          name: getDict().seriesMedium,
        },
        {
          key: "small_order_net_inflow_amount",
          name: getDict().seriesSmall,
        },
      ];

      const option = {
        tooltip: {
          trigger: "axis",
          valueFormatter: (value) =>
            formatNumber(value, { maximumFractionDigits: 2 }),
        },
        legend: {
          data: seriesDefs.map((s) => s.name),
          top: 0,
        },
        grid: { left: 48, right: 24, top: 40, bottom: 40 },
        xAxis: {
          type: "category",
          boundaryGap: false,
          data: xAxis,
        },
        yAxis: {
          type: "value",
          axisLabel: {
            formatter: (value) =>
              formatNumber(value, { notation: "compact", maximumFractionDigits: 2 }),
          },
        },
        series: seriesDefs.map((series) => ({
          name: series.name,
          type: "line",
          smooth: true,
          showSymbol: false,
          emphasis: { focus: "series" },
          lineStyle: { width: 2 },
          data: sorted.map((item) => {
            const value = getRecordValue(item, series.key);
            return value === undefined ? null : value;
          }),
        })),
      };

      instance.clear();
      instance.setOption(option, { notMerge: true });
      requestAnimationFrame(() => instance.resize());
      elements.chartEmpty.classList.add("hidden");
      elements.chartContainer.classList.remove("hidden");
    })
    .catch(() => {
      disposeChart();
      elements.chartContainer.classList.add("hidden");
      elements.chartEmpty.classList.remove("hidden");
    });
}

function createSummaryItem(label, value) {
  const wrapper = document.createElement("div");
  wrapper.className = "flow-card__summary-item";
  const labelEl = document.createElement("span");
  labelEl.textContent = label;
  const valueEl = document.createElement("strong");
  valueEl.textContent = value;
  wrapper.append(labelEl, valueEl);
  return wrapper;
}

function createStat(label, amount, ratio) {
  const stat = document.createElement("div");
  stat.className = "flow-card__stat";

  const labelEl = document.createElement("span");
  labelEl.textContent = label;

  const amountEl = document.createElement("span");
  amountEl.className = "flow-card__stat-value";
  amountEl.textContent = formatNumber(amount, {
    notation: "compact",
    maximumFractionDigits: 2,
  });

  stat.append(labelEl, amountEl);

  if (ratio !== undefined && ratio !== null) {
    const ratioEl = document.createElement("span");
    ratioEl.className = "flow-card__stat-ratio";
    ratioEl.textContent = formatPercent(ratio, 2);
    const numericRatio = Number(ratio);
    if (Number.isFinite(numericRatio) && numericRatio !== 0) {
      ratioEl.classList.add(numericRatio > 0 ? "positive" : "negative");
    }
    stat.append(ratioEl);
  }

  return stat;
}

function renderCards(items) {
  if (!elements.cardsContainer) {
    return;
  }
  const container = elements.cardsContainer;
  container.innerHTML = "";

  if (!items || !items.length) {
    const message =
      (currentLang === "zh" ? container.dataset.emptyZh : container.dataset.emptyEn) ||
      (currentLang === "zh" ? "暂无大盘资金流数据。" : "No market fund flow data.");
    const empty = document.createElement("div");
    empty.className = "flow-card__empty";
    empty.textContent = message;
    container.appendChild(empty);
    return;
  }

  const dict = getDict();
  const visible = items.slice(0, MAX_CARD_ITEMS);
  const fragment = document.createDocumentFragment();

  visible.forEach((item) => {
    const tradeDate =
      formatDate(getRecordValue(item, "trade_date") || getRecordValue(item, "tradeDate")) || "--";
    const shClose = getRecordValue(item, "shanghai_close");
    const shChange = getRecordValue(item, "shanghai_change_percent");
    const szClose = getRecordValue(item, "shenzhen_close");
    const szChange = getRecordValue(item, "shenzhen_change_percent");
    const mainNet = getRecordValue(item, "main_net_inflow_amount");
    const mainRatio = getRecordValue(item, "main_net_inflow_ratio");
    const largeNet = getRecordValue(item, "large_order_net_inflow_amount");
    const largeRatio = getRecordValue(item, "large_order_net_inflow_ratio");
    const mediumNet = getRecordValue(item, "medium_order_net_inflow_amount");
    const mediumRatio = getRecordValue(item, "medium_order_net_inflow_ratio");
    const smallNet = getRecordValue(item, "small_order_net_inflow_amount");
    const smallRatio = getRecordValue(item, "small_order_net_inflow_ratio");

    const card = document.createElement("article");
    card.className = "flow-card";

    const header = document.createElement("div");
    header.className = "flow-card__header";
    const heading = document.createElement("div");
    heading.className = "flow-card__heading";
    const titleEl = document.createElement("div");
    titleEl.className = "flow-card__title";
    titleEl.textContent = tradeDate;
    heading.appendChild(titleEl);

    if (dict.cardTitle) {
      const subtitle = document.createElement("span");
      subtitle.className = "flow-card__subtitle";
      subtitle.textContent = dict.cardTitle;
      heading.appendChild(subtitle);
    }

    header.appendChild(heading);
    card.appendChild(header);

    const summary = document.createElement("div");
    summary.className = "flow-card__summary";
    summary.append(
      createSummaryItem(dict.colShanghaiClose || "Shanghai Close", formatNumber(shClose, { maximumFractionDigits: 2 })),
      createSummaryItem(dict.colShanghaiChange || "Shanghai %", formatPercent(shChange, 2)),
      createSummaryItem(dict.colShenzhenClose || "Shenzhen Close", formatNumber(szClose, { maximumFractionDigits: 2 })),
      createSummaryItem(dict.colShenzhenChange || "Shenzhen %", formatPercent(szChange, 2))
    );
    card.appendChild(summary);

    const stats = document.createElement("div");
    stats.className = "flow-card__stats";
    stats.append(
      createStat(dict.cardMainNet || "Main Net", mainNet, mainRatio),
      createStat(dict.cardLargeNet || "Large Net", largeNet, largeRatio),
      createStat(dict.cardMediumNet || "Medium Net", mediumNet, mediumRatio),
      createStat(dict.cardSmallNet || "Small Net", smallNet, smallRatio)
    );
    card.appendChild(stats);

    const trend = document.createElement("div");
    trend.className = "flow-card__trend";
    const badge = document.createElement("span");
    badge.className = "flow-card__trend-badge";
    const badgeValue = Number(mainNet);
    if (Number.isFinite(badgeValue) && badgeValue < 0) {
      badge.classList.add("negative");
    }
    badge.textContent = badgeValue > 0 ? "↑" : badgeValue < 0 ? "↓" : "·";

    const trendLabel = document.createElement("span");
    trendLabel.textContent = dict.cardTrend || "Trend";

    const trendAmount = document.createElement("span");
    trendAmount.className = "flow-card__trend-value";
    trendAmount.textContent = formatNumber(mainNet, { notation: "compact", maximumFractionDigits: 2 });

    const trendRatio = document.createElement("span");
    trendRatio.className = "flow-card__trend-value";
    trendRatio.textContent = formatPercent(mainRatio, 2);

    trend.append(badge, trendLabel, trendAmount, trendRatio);
    card.appendChild(trend);

    fragment.appendChild(card);
  });

  container.appendChild(fragment);
}

function onDateChange() {
  state.startDate = elements.startDate?.value || "";
  state.endDate = elements.endDate?.value || "";
  validateDateRange();
  syncDateInputs();
  loadMarketFundFlow();
}

function resetFilters() {
  state.startDate = "";
  state.endDate = "";
  syncDateInputs();
  loadMarketFundFlow();
}

async function loadMarketFundFlow() {
  try {
    validateDateRange();
    syncDateInputs();
    const params = new URLSearchParams();
    params.set("limit", "200");
    if (state.startDate) {
      params.set("startDate", state.startDate);
    }
    if (state.endDate) {
      params.set("endDate", state.endDate);
    }

    const response = await fetch(`${API_BASE}/fund-flow/market?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    applyTranslations();
  } catch (error) {
    console.error("Failed to load market fund flow data", error);
    currentItems = [];
    applyTranslations();
  }
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      if (btn.dataset.lang && btn.dataset.lang !== currentLang) {
        currentLang = btn.dataset.lang;
        persistLanguage(currentLang);
        applyTranslations();
      }
    });
  });

  if (elements.startDate) {
    elements.startDate.addEventListener("change", onDateChange);
  }
  if (elements.endDate) {
    elements.endDate.addEventListener("change", onDateChange);
  }
  if (elements.resetButton) {
    elements.resetButton.addEventListener("click", resetFilters);
  }
}

function initialize() {
  bindEvents();
  applyTranslations();
  loadMarketFundFlow();
}

initialize();
