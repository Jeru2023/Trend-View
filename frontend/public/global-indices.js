const translations = getTranslations("globalIndices");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
let echartsLoader = null;
const DEFAULT_SYMBOL = "^IXIC";
const HISTORY_LIMIT = 365;
const DEFAULT_WINDOW_DAYS = 90;

let currentLang = getInitialLanguage();
let availableIndices = [];
const chartState = {
  selectedCode: null,
  cache: new Map(),
};
let chartInstance = null;
let resizeListenerAttached = false;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("global-index-tbody"),
  lastSynced: document.getElementById("global-index-last-synced"),
  refreshButton: document.getElementById("global-index-refresh"),
  selector: document.getElementById("global-index-selector"),
  chartContainer: document.getElementById("global-index-chart"),
  chartEmpty: document.getElementById("global-index-chart-empty"),
  chartUpdated: document.getElementById("global-index-chart-updated"),
};

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
  const browserLang = (navigator.language || "").toLowerCase();
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

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric.toFixed(2)}%`;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    })}`;
  } catch (error) {
    return String(value);
  }
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return date.toLocaleDateString(locale);
  } catch (error) {
    return String(value);
  }
}

function formatVolumeLabel(value) {
  if (!Number.isFinite(Number(value))) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(Number(value));
}

function safeNumber(value) {
  if (value === null || value === undefined) {
    return NaN;
  }
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : NaN;
}

function normalizeHistoryItems(items = []) {
  if (!Array.isArray(items) || !items.length) {
    return { asc: [], desc: [] };
  }

  const mapped = items
    .map((item) => ({
      raw: item,
      date: item.tradeDate || item.trade_date,
      open: safeNumber(item.openPrice ?? item.open_price),
      close: safeNumber(item.closePrice ?? item.close_price),
      low: safeNumber(item.lowPrice ?? item.low_price),
      high: safeNumber(item.highPrice ?? item.high_price),
      volume: safeNumber(item.volume),
      prevClose: safeNumber(item.prevClose ?? item.prev_close),
      changeAmount: safeNumber(item.changeAmount ?? item.change_amount),
      changePercent: safeNumber(item.changePercent ?? item.change_percent),
    }))
    .filter((row) => row.date);

  mapped.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

  mapped.forEach((row, idx) => {
    const prevRow = idx > 0 ? mapped[idx - 1] : null;
    let prevClose = row.prevClose;
    if (!Number.isFinite(prevClose) && prevRow && Number.isFinite(prevRow.close)) {
      prevClose = prevRow.close;
    } else if (
      prevRow &&
      Number.isFinite(prevClose) &&
      Number.isFinite(row.close) &&
      Math.abs(prevClose - row.close) < 1e-6 &&
      Math.abs(prevRow.close - row.close) > 1e-6
    ) {
      prevClose = prevRow.close;
    }

    let changeAmount = row.changeAmount;
    if (!Number.isFinite(changeAmount) || Math.abs(changeAmount) < 1e-6) {
      if (Number.isFinite(prevClose) && Number.isFinite(row.close)) {
        changeAmount = row.close - prevClose;
      } else if (Number.isFinite(row.close) && Number.isFinite(row.open)) {
        changeAmount = row.close - row.open;
      }
    }

    let changePercent = row.changePercent;
    if (!Number.isFinite(changePercent) || Math.abs(changePercent) < 1e-6) {
      if (Number.isFinite(changeAmount) && Number.isFinite(prevClose) && prevClose !== 0) {
        changePercent = (changeAmount / prevClose) * 100;
      } else if (Number.isFinite(row.close) && Number.isFinite(row.open) && row.open !== 0) {
        changePercent = ((row.close - row.open) / row.open) * 100;
      }
    }

    row.prevClose = prevClose;
    row.changeAmount = changeAmount;
    row.changePercent = changePercent;
  });

  const desc = mapped
    .slice()
    .reverse()
    .map((row) => ({
      ...row.raw,
      tradeDate: row.date,
      openPrice: row.open,
      highPrice: row.high,
      lowPrice: row.low,
      closePrice: row.close,
      volume: row.volume,
      prevClose: row.prevClose,
      changeAmount: row.changeAmount,
      changePercent: row.changePercent,
    }));

  return { asc: mapped, desc };
}

function readValue(item, camelKey, snakeKey) {
  if (!item) {
    return undefined;
  }
  if (camelKey && Object.prototype.hasOwnProperty.call(item, camelKey) && item[camelKey] !== null && item[camelKey] !== undefined) {
    return item[camelKey];
  }
  if (snakeKey && Object.prototype.hasOwnProperty.call(item, snakeKey) && item[snakeKey] !== null && item[snakeKey] !== undefined) {
    return item[snakeKey];
  }
  return camelKey ? item[camelKey] ?? item[snakeKey] : item[snakeKey];
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = dict.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      if (key === "refreshButton" && elements.refreshButton?.dataset.loading === "1") {
        el.textContent = dict.refreshing || value;
      } else {
        el.textContent = value;
      }
    }
  });

  document.querySelectorAll(".lang-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  renderSelector();
  if (chartState.selectedCode) {
    const cached = chartState.cache.get(chartState.selectedCode);
    if (cached?.normalizedAsc) {
      renderChart(cached.normalizedAsc);
      renderHistoryTable(cached.normalizedDesc);
    }
  }
}

function renderHistoryEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 7;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(row);
}

function renderHistoryTable(items = []) {
  if (!elements.tableBody) {
    return;
  }
  if (!items.length) {
    const message =
      elements.tableBody.dataset[`empty${currentLang.toUpperCase()}`] ||
      getDict().historyEmpty ||
      getDict().chartEmpty ||
      "No history data.";
    renderHistoryEmpty(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    const changeValue = safeNumber(item.changeAmount ?? item.change_amount);
    const percentValue = safeNumber(item.changePercent ?? item.change_percent);
    const row = document.createElement("tr");
    const columns = [
      formatDate(item.tradeDate || item.trade_date),
      formatNumber(item.openPrice ?? item.open_price, { maximumFractionDigits: 2 }),
      formatNumber(item.highPrice ?? item.high_price, { maximumFractionDigits: 2 }),
      formatNumber(item.lowPrice ?? item.low_price, { maximumFractionDigits: 2 }),
      formatNumber(item.closePrice ?? item.close_price, { maximumFractionDigits: 2 }),
      formatNumber(item.changeAmount ?? item.change_amount, { maximumFractionDigits: 2 }),
      formatPercent(item.changePercent ?? item.change_percent),
    ];
    columns.forEach((value, index) => {
      const cell = document.createElement("td");
      if (index >= 5 && typeof value === "string") {
        const changeClass =
          index === 5
            ? changeValue > 0
              ? "text-up"
              : changeValue < 0
                ? "text-down"
                : ""
            : percentValue > 0
              ? "text-up"
              : percentValue < 0
                ? "text-down"
                : "";
        if (changeClass) {
          cell.innerHTML = `<span class="${changeClass}">${value}</span>`;
        } else {
          cell.textContent = value;
        }
      } else {
        cell.textContent = typeof value === "string" ? value : String(value ?? "--");
      }
      row.appendChild(cell);
    });
    fragment.appendChild(row);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function updateLastSynced(timestamp) {
  if (!elements.lastSynced) {
    return;
  }
  elements.lastSynced.textContent = timestamp ? formatDateTime(timestamp) : "--";
}

async function fetchGlobalIndices() {
  const dict = getDict();
  renderHistoryEmpty(dict.loading || "Loading...");

  try {
    const response = await fetch(`${API_BASE}/macro/global-indices`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    const items = Array.isArray(data.items) ? data.items : [];
    updateAvailableIndices(items, { forceReload: true });
    updateLastSynced(data.lastSyncedAt || data.last_synced_at);
  } catch (error) {
    console.error("Failed to fetch global indices:", error);
    renderHistoryEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
    updateAvailableIndices([]);
  }
}

function setChartUpdatedText(value) {
  if (!elements.chartUpdated) {
    return;
  }
  elements.chartUpdated.textContent = value ? formatDate(value) : "--";
}

function showChartMessage(message) {
  if (!elements.chartEmpty) {
    return;
  }
  if (!message) {
    elements.chartEmpty.classList.add("hidden");
    return;
  }
  elements.chartEmpty.textContent = message;
  elements.chartEmpty.classList.remove("hidden");
  if (chartInstance) {
    chartInstance.clear();
  }
}

function disposeChartInstance() {
  if (chartInstance) {
    chartInstance.dispose();
    chartInstance = null;
  }
}

function renderSelector() {
  if (!elements.selector) {
    return;
  }
  const fragment = document.createDocumentFragment();
  availableIndices.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "index-chip";
    button.dataset.code = item.code;
    button.textContent = item.name || item.code;
    if (item.code === chartState.selectedCode) {
      button.classList.add("index-chip--active");
      button.setAttribute("aria-pressed", "true");
    } else {
      button.setAttribute("aria-pressed", "false");
    }
    button.addEventListener("click", () => {
      if (item.code !== chartState.selectedCode) {
        loadHistoryForCode(item.code);
      }
    });
    fragment.appendChild(button);
  });
  elements.selector.innerHTML = "";
  if (availableIndices.length) {
    elements.selector.appendChild(fragment);
  }
}

function attachResizeListener() {
  if (resizeListenerAttached) {
    return;
  }
  resizeListenerAttached = true;
  window.addEventListener(
    "resize",
    () => {
      if (chartInstance) {
        chartInstance.resize();
      }
    },
    { passive: true },
  );
}

function updateAvailableIndices(items = [], { forceReload = false } = {}) {
  availableIndices = items
    .filter((item) => item && item.code)
    .map((item) => ({
      code: item.code,
      name: item.name,
    }));

  if (!availableIndices.length) {
    chartState.selectedCode = null;
    chartState.cache.clear();
    renderSelector();
    disposeChartInstance();
    showChartMessage(getDict().chartEmpty || getDict().empty || "No chart data available.");
    setChartUpdatedText(null);
    return;
  }

  if (!chartState.selectedCode || !availableIndices.some((entry) => entry.code === chartState.selectedCode)) {
    const preferred =
      availableIndices.find((entry) => entry.code === DEFAULT_SYMBOL) ||
      (availableIndices.length ? availableIndices[0] : null);
    chartState.selectedCode = preferred ? preferred.code : null;
  }

  renderSelector();
  loadHistoryForCode(chartState.selectedCode, { force: forceReload });
}

async function fetchGlobalIndexHistory(code, limit = 260) {
  const params = new URLSearchParams({ code, limit: String(limit) });
  const response = await fetch(`${API_BASE}/macro/global-indices/history?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

async function loadHistoryForCode(code, { force } = { force: false }) {
  if (!code) {
    return;
  }
  chartState.selectedCode = code;
  renderSelector();

  const dict = getDict();
  const cached = chartState.cache.get(code);
  if (cached && cached.normalizedAsc && !force) {
    renderChart(cached.normalizedAsc);
    renderHistoryTable(cached.normalizedDesc);
    return;
  }
  if (cached && !cached.normalizedAsc) {
    chartState.cache.delete(code);
  }

  showChartMessage(dict.chartLoading || dict.loading || "Loading...");
  setChartUpdatedText(null);

  try {
    const payload = await fetchGlobalIndexHistory(code, HISTORY_LIMIT);
    const normalized = normalizeHistoryItems(payload.items || []);
    chartState.cache.set(code, { payload, normalizedAsc: normalized.asc, normalizedDesc: normalized.desc });
    if (chartState.selectedCode === code) {
      renderChart(normalized.asc);
      renderHistoryTable(normalized.desc);
    }
  } catch (error) {
    console.error("Failed to load global index history:", error);
    if (chartState.selectedCode === code) {
      showChartMessage(dict.chartError || "Unable to load chart.");
      renderHistoryEmpty(error?.message || dict.historyEmpty || "Failed to load history.");
    }
  }
}

function renderChart(rows = []) {
  if (!elements.chartContainer) {
    return;
  }
  const dict = getDict();
  if (!Array.isArray(rows) || !rows.length) {
    showChartMessage(dict.chartEmpty || "No chart data available.");
    disposeChartInstance();
    setChartUpdatedText(null);
    return;
  }

  ensureEchartsLoaded()
    .then(() => {
      const upColor = "#3066BE";
      const downColor = "#E07A1F";
      const categories = rows.map((row) => row.date);
      const candleValues = rows.map((row) => [row.open, row.close, row.low, row.high]);
      const volumeValues = rows.map((row) => ({
        value: Number.isFinite(row.volume) ? row.volume : 0,
        itemStyle: { color: row.close >= row.open ? upColor : downColor },
      }));

      if (!chartInstance) {
        chartInstance = window.echarts.init(elements.chartContainer, undefined, { renderer: "canvas" });
      }

      showChartMessage("");
      const windowSize = DEFAULT_WINDOW_DAYS;
      const startPercent =
        categories.length > windowSize ? Math.max(0, 100 - Math.round((windowSize / categories.length) * 100)) : 0;

      const option = {
        animation: false,
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter(params) {
            if (!Array.isArray(params) || !params.length) {
              return "";
            }
            const candlePoint = params.find((item) => item.seriesType === "candlestick");
            const volumePoint = params.find((item) => item.seriesName === (dict.volumeSeriesName || "Volume"));
            let dataIndex = -1;
            if (typeof candlePoint?.dataIndex === "number") {
              dataIndex = candlePoint.dataIndex;
            } else if (typeof volumePoint?.dataIndex === "number") {
              dataIndex = volumePoint.dataIndex;
            } else if (typeof params[0]?.dataIndex === "number") {
              dataIndex = params[0].dataIndex;
            }
            if (dataIndex < 0) {
              dataIndex = 0;
            }
            const row = rows[dataIndex] || {};
            const axisValue = categories[dataIndex] ?? params[0]?.axisValue ?? "";
            const lines = [axisValue];
            const open = row.open;
            const close = row.close;
            const high = row.high;
            const low = row.low;

            lines.push(`${dict.tooltipOpen || "Open"}: ${formatNumber(open, { maximumFractionDigits: 2 })}`);
            lines.push(`${dict.tooltipClose || "Close"}: ${formatNumber(close, { maximumFractionDigits: 2 })}`);
            lines.push(`${dict.tooltipHigh || "High"}: ${formatNumber(high, { maximumFractionDigits: 2 })}`);
            lines.push(`${dict.tooltipLow || "Low"}: ${formatNumber(low, { maximumFractionDigits: 2 })}`);

            if (Number.isFinite(row.changeAmount)) {
              const changeClass = row.changeAmount > 0 ? "text-up" : row.changeAmount < 0 ? "text-down" : "";
              const formatted = formatNumber(row.changeAmount, { maximumFractionDigits: 2 });
              lines.push(
                `${dict.tooltipChangeAmount || "Change"}: ${
                  changeClass ? `<span class="${changeClass}">${formatted}</span>` : formatted
                }`,
              );
            }
            if (Number.isFinite(row.changePercent)) {
              const changeClass = row.changePercent > 0 ? "text-up" : row.changePercent < 0 ? "text-down" : "";
              const formatted = `${row.changePercent >= 0 ? "+" : ""}${Math.abs(row.changePercent).toFixed(2)}%`;
              lines.push(
                `${dict.tooltipChange || "Change (%)"}: ${
                  changeClass ? `<span class="${changeClass}">${formatted}</span>` : formatted
                }`,
              );
            }

            const volumeValue =
              (volumePoint && typeof volumePoint.value === "number" && Number.isFinite(volumePoint.value)
                ? volumePoint.value
                : volumePoint && volumePoint.data && Number.isFinite(volumePoint.data.value)
                  ? volumePoint.data.value
                  : Number.isFinite(row.volume)
                    ? row.volume
                    : null);
            if (Number.isFinite(volumeValue)) {
              lines.push(`${dict.tooltipVolume || "Volume"}: ${formatVolumeLabel(volumeValue)}`);
            }
            return lines.join("<br>");
          },
        },
        grid: [
          { left: 60, right: 24, top: 20, height: "60%" },
          { left: 60, right: 24, top: "72%", height: "18%" },
        ],
        xAxis: [
          {
            type: "category",
            data: categories,
            boundaryGap: false,
            axisLine: { lineStyle: { color: "#CBD5F5" } },
            axisLabel: { color: "#475569" },
          },
          {
            type: "category",
            gridIndex: 1,
            data: categories,
            boundaryGap: false,
            axisLine: { lineStyle: { color: "transparent" } },
            axisLabel: { show: false },
          },
        ],
        yAxis: [
          {
            scale: true,
            axisLine: { lineStyle: { color: "transparent" } },
            splitLine: { lineStyle: { color: "rgba(148,163,184,0.25)" } },
            axisLabel: { color: "#475569" },
          },
          {
            gridIndex: 1,
            axisLine: { lineStyle: { color: "transparent" } },
            splitLine: { show: false },
            axisLabel: {
              color: "#94a3b8",
              formatter: (value) => formatVolumeLabel(value),
            },
          },
        ],
        dataZoom: [
          {
            type: "slider",
            start: startPercent,
            end: 100,
            height: 18,
            bottom: 8,
            borderColor: "transparent",
            xAxisIndex: [0, 1],
            zoomLock: false,
            filterMode: "filter",
          },
        ],
        series: [
          {
            name: dict.candleSeriesName || "Price",
            type: "candlestick",
            data: candleValues,
            itemStyle: {
              color: upColor,
              color0: downColor,
              borderColor: upColor,
              borderColor0: downColor,
            },
          },
          {
            name: dict.volumeSeriesName || "Volume",
            type: "bar",
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: volumeValues,
          },
        ],
      };

      chartInstance.setOption(option, true);
      attachResizeListener();
      const latest = rows[rows.length - 1];
      setChartUpdatedText(latest?.date);
    })
    .catch((error) => {
      console.error("Failed to render chart:", error);
      showChartMessage(getDict().chartError || "Unable to load chart.");
      disposeChartInstance();
      setChartUpdatedText(null);
    });
}

function setRefreshLoading(isLoading) {
  const button = elements.refreshButton;
  if (!button) {
    return;
  }
  const dict = getDict();
  if (isLoading) {
    button.dataset.loading = "1";
    button.disabled = true;
    button.textContent = dict.refreshing || dict.refreshButton || "Refreshing...";
  } else {
    delete button.dataset.loading;
    button.disabled = false;
    button.textContent = dict.refreshButton || "Refresh";
  }
}

async function triggerManualSync() {
  if (!elements.refreshButton || elements.refreshButton.dataset.loading === "1") {
    return;
  }
  const dict = getDict();
  setRefreshLoading(true);
  try {
    const response = await fetch(`${API_BASE}/control/sync/global-indices`, { method: "POST" });
    if (!response.ok) {
      let detail = response.statusText || `HTTP ${response.status}`;
      try {
        const payload = await response.json();
        if (payload && typeof payload.detail === "string") {
          detail = payload.detail;
        }
      } catch (error) {
        /* no-op parsing failure */
      }
      throw new Error(detail);
    }
    await fetchGlobalIndices();
  } catch (error) {
    console.error("Manual global index sync failed:", error);
    renderHistoryEmpty(error?.message || dict.loading || "Failed to load data");
    updateLastSynced(null);
    updateAvailableIndices([]);
  } finally {
    setRefreshLoading(false);
  }
}

function bindLanguageButtons() {
  elements.langButtons.forEach((btn) => {
    btn.onclick = () => {
      const lang = btn.dataset.lang;
      if (lang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
        fetchGlobalIndices();
      }
    };
  });
}

function bindRefreshButton() {
  if (!elements.refreshButton) {
    return;
  }
  elements.refreshButton.addEventListener("click", (event) => {
    event.preventDefault();
    triggerManualSync();
  });
}

function initialize() {
  applyTranslations();
  bindLanguageButtons();
  bindRefreshButton();
  fetchGlobalIndices();
}

document.addEventListener("DOMContentLoaded", initialize);

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
