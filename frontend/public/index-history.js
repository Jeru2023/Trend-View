const translations = getTranslations("indexHistory");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
let echartsLoader = null;

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

let currentLang = getInitialLanguage();
let selectedCode = null;
let availableIndices = [];
let latestItems = [];
let chartInstance = null;
let resizeListenerAttached = false;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  selector: document.getElementById("index-history-selector"),
  lastUpdated: document.getElementById("index-history-last-updated"),
  refreshButton: document.getElementById("index-history-refresh"),
  status: document.getElementById("index-history-status"),
  tableBody: document.getElementById("index-history-tbody"),
  chartContainer: document.getElementById("index-history-chart"),
  chartEmpty: document.getElementById("index-history-chart-empty"),
};

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

function formatVolumeLabel(value, digits = 1) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const abs = Math.abs(numeric);
  if (currentLang === "zh") {
    const hundredMillion = 100000000;
    const tenThousand = 10000;
    if (abs >= hundredMillion) {
      return `${(numeric / hundredMillion).toFixed(digits)}亿`;
    }
    if (abs >= tenThousand) {
      return `${(numeric / tenThousand).toFixed(digits)}万`;
    }
  } else {
    const billion = 1000000000;
    const million = 1000000;
    const thousand = 1000;
    if (abs >= billion) {
      return `${(numeric / billion).toFixed(digits)}B`;
    }
    if (abs >= million) {
      return `${(numeric / million).toFixed(digits)}M`;
    }
    if (abs >= thousand) {
      return `${(numeric / thousand).toFixed(digits)}K`;
    }
  }
  return formatNumber(numeric, { maximumFractionDigits: 0 });
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.title = dict.title;
  setStatus("");

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

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  renderSelector();
  renderTable(latestItems);
  renderChart(latestItems);
}

function setButtonLoading(isLoading) {
  if (!elements.refreshButton) {
    return;
  }
  const dict = getDict();
  elements.refreshButton.disabled = isLoading;
  elements.refreshButton.dataset.loading = isLoading ? "1" : "0";
  elements.refreshButton.textContent = isLoading ? dict.refreshing || "Refreshing…" : dict.refreshButton;
}

function setStatus(message, tone = "info") {
  if (!elements.status) {
    return;
  }
  if (!message) {
    elements.status.textContent = "";
    elements.status.removeAttribute("data-tone");
    return;
  }
  elements.status.textContent = message;
  elements.status.dataset.tone = tone;
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
    if (item.code === selectedCode) {
      button.classList.add("index-chip--active");
      button.setAttribute("aria-pressed", "true");
    } else {
      button.setAttribute("aria-pressed", "false");
    }
    button.dataset.code = item.code;
    button.textContent = item.name || item.code;
    button.addEventListener("click", () => {
      if (item.code !== selectedCode) {
        loadIndexHistory(item.code, { force: true });
      }
    });
    fragment.appendChild(button);
  });
  elements.selector.innerHTML = "";
  elements.selector.appendChild(fragment);
}

function renderEmptyRow(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 9;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(row);
}

function renderTable(items = []) {
  if (!elements.tableBody) {
    return;
  }
  if (!Array.isArray(items) || items.length === 0) {
    const key = `empty${currentLang.toUpperCase()}`;
    const fallback = currentLang === "zh" ? "暂无指数行情数据。" : "No historical records available.";
    renderEmptyRow(elements.tableBody.dataset[key] || fallback);
    return;
  }

  const fragment = document.createDocumentFragment();
  [...items]
    .slice()
    .reverse()
    .forEach((item) => {
      const row = document.createElement("tr");
      const columns = [
        formatDate(item.tradeDate || item.trade_date),
        formatNumber(item.close, { maximumFractionDigits: 2 }),
        formatNumber(item.open, { maximumFractionDigits: 2 }),
        formatNumber(item.high, { maximumFractionDigits: 2 }),
        formatNumber(item.low, { maximumFractionDigits: 2 }),
        formatPercent(item.pctChange ?? item.pct_change),
        formatNumber(item.changeAmount ?? item.change_amount, { maximumFractionDigits: 2 }),
        formatVolumeLabel(item.volume, 2),
      ];
      columns.forEach((text) => {
        const cell = document.createElement("td");
        cell.textContent = text;
        row.appendChild(cell);
      });
      fragment.appendChild(row);
    });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function computeLatestDate(items) {
  if (!items.length) {
    return "--";
  }
  const lastItem = items[items.length - 1];
  return formatDate(lastItem.tradeDate || lastItem.trade_date);
}

function renderChart(items = []) {
  if (!elements.chartContainer || !elements.chartEmpty) {
    return;
  }
  if (!items.length) {
    elements.chartEmpty.classList.remove("hidden");
    if (chartInstance) {
      chartInstance.dispose();
      chartInstance = null;
    }
    return;
  }

  ensureEchartsLoaded()
    .then(() => {
      const dict = getDict();
      const upColor = "#3066BE";
      const downColor = "#E07A1F";
      const maxPoints = 260;
      const recentItems = items.slice(-maxPoints);

      const categories = [];
      const candleValues = [];
      const volumeValues = [];

      recentItems.forEach((item) => {
        const tradeDate = item.tradeDate || item.trade_date;
        const open = Number(item.open);
        const close = Number(item.close);
        const low = Number(item.low);
        const high = Number(item.high);
        if (
          !Number.isFinite(open) ||
          !Number.isFinite(close) ||
          !Number.isFinite(high) ||
          !Number.isFinite(low)
        ) {
          return;
        }
        const volumeRaw = Number(item.volume);
        const rising = close >= open;
        categories.push(tradeDate);
        candleValues.push([open, close, low, high]);
        volumeValues.push({
          value: Number.isFinite(volumeRaw) ? volumeRaw : 0,
          itemStyle: { color: rising ? upColor : downColor },
        });
      });

      if (!categories.length) {
        elements.chartEmpty.classList.remove("hidden");
        if (chartInstance) {
          chartInstance.dispose();
          chartInstance = null;
        }
        return;
      }

      if (!chartInstance) {
        chartInstance = window.echarts.init(elements.chartContainer, undefined, { renderer: "canvas" });
      }

      elements.chartEmpty.classList.add("hidden");

      const windowSize = 180;
      const startPercent =
        categories.length > windowSize ? Math.max(0, 100 - Math.round((windowSize / categories.length) * 100)) : 0;

      const option = {
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter(params) {
            if (!Array.isArray(params) || !params.length) {
              return "";
            }
            const axisValue = params[0]?.axisValue ?? "";
            const lines = [axisValue];
            const candle = params.find((item) => item.seriesType === "candlestick");
            if (candle && Array.isArray(candle.data)) {
              const [open, close, low, high] = candle.data;
              lines.push(`${dict.tooltipOpen || "Open"}: ${formatNumber(open, { maximumFractionDigits: 2 })}`);
              lines.push(`${dict.tooltipClose || "Close"}: ${formatNumber(close, { maximumFractionDigits: 2 })}`);
              lines.push(`${dict.tooltipHigh || "High"}: ${formatNumber(high, { maximumFractionDigits: 2 })}`);
              lines.push(`${dict.tooltipLow || "Low"}: ${formatNumber(low, { maximumFractionDigits: 2 })}`);
              const sourceIdx = candle.dataIndex;
              const source = Number.isInteger(sourceIdx) ? recentItems[sourceIdx] : null;
              const pctFromSource = source
                ? Number(source.pctChange ?? source.pct_change)
                : null;
              if (Number.isFinite(pctFromSource)) {
                const pct = pctFromSource;
                const formatted = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
                const color = pct > 0 ? upColor : pct < 0 ? downColor : "#64748b";
                lines.push(
                  `${dict.tooltipChange || "Change (%)"}: <span style="color:${color};font-weight:600">${formatted}</span>`,
                );
              } else if (Number.isFinite(open) && Number.isFinite(close) && open !== 0) {
                const pct = ((close - open) / open) * 100;
                if (Number.isFinite(pct)) {
                  const formatted = `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
                  const color = pct > 0 ? upColor : pct < 0 ? downColor : "#64748b";
                  lines.push(
                    `${dict.tooltipChange || "Change (%)"}: <span style="color:${color};font-weight:600">${formatted}</span>`,
                  );
                }
              }
            }
            const volume = params.find((item) => item.seriesName === (dict.volumeSeriesName || "Volume"));
            const volumeValue =
              (volume && typeof volume.value === "number" && Number.isFinite(volume.value)
                ? volume.value
                : volume && volume.data && Number.isFinite(volume.data.value)
                  ? volume.data.value
                  : null);
            if (Number.isFinite(volumeValue)) {
              lines.push(`${dict.tooltipVolume || "Volume"}: ${formatVolumeLabel(volumeValue, 2)}`);
            }
            return lines.join("<br />");
          },
        },
        axisPointer: {
          link: [{ xAxisIndex: [0, 1] }],
        },
        grid: [
          { left: 48, right: 16, top: 24, height: 240 },
          { left: 48, right: 16, top: 286, height: 80 },
        ],
        xAxis: [
          {
            type: "category",
            boundaryGap: true,
            data: categories,
            axisTick: { show: false },
            axisLabel: {
              formatter(value) {
                return value ? value.slice(5) : value;
              },
            },
          },
          {
            type: "category",
            gridIndex: 1,
            boundaryGap: true,
            data: categories,
            axisTick: { show: false },
            axisLabel: { show: false },
          },
        ],
        yAxis: [
          {
            scale: true,
            axisLabel: {
              formatter(value) {
                return formatNumber(value, { maximumFractionDigits: 0 });
              },
            },
            splitLine: {
              lineStyle: { color: "rgba(148, 163, 184, 0.25)" },
            },
          },
          {
            scale: true,
            gridIndex: 1,
            axisLabel: {
              formatter(value) {
                return formatVolumeLabel(value, 1);
              },
            },
            axisTick: { show: false },
            axisLine: { show: false },
            splitLine: { show: false },
          },
        ],
        dataZoom: [
          {
            type: "inside",
            xAxisIndex: [0, 1],
            start: startPercent,
            end: 100,
            minSpan: 10,
          },
          {
            type: "slider",
            xAxisIndex: [0, 1],
            start: startPercent,
            end: 100,
            top: 376,
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
            emphasis: {
              itemStyle: {
                borderWidth: 1,
              },
            },
          },
          {
            name: dict.volumeSeriesName || "Volume",
            type: "bar",
            xAxisIndex: 1,
            yAxisIndex: 1,
            barWidth: "60%",
            data: volumeValues,
          },
        ],
      };

      chartInstance.setOption(option, true);

      if (!resizeListenerAttached) {
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
    })
    .catch((error) => {
      console.error("Failed to render index chart:", error);
      elements.chartEmpty.classList.remove("hidden");
      elements.chartEmpty.textContent = getDict().chartError || "Unable to render chart.";
    });
}

async function loadIndexHistory(code, { force = false, skipButtonState = false } = {}) {
  const targetCode = code || selectedCode || null;
  const url = new URL(`${API_BASE}/markets/index-history`);
  if (targetCode) {
    url.searchParams.set("indexCode", targetCode);
  }
  url.searchParams.set("limit", "600");

  if (!force && elements.refreshButton?.dataset.loading === "1") {
    return;
  }

  const shouldToggleButton = !skipButtonState;
  if (shouldToggleButton) {
    setButtonLoading(true);
  }
  const dict = getDict();

  try {
    const response = await fetch(url.toString(), { cache: "no-store" });
    if (!response.ok) {
      throw new Error(dict.loadFailed || `Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    availableIndices = Array.isArray(payload.availableIndices) ? payload.availableIndices : availableIndices;
    selectedCode = payload.indexCode || targetCode || (availableIndices[0] && availableIndices[0].code) || null;
    latestItems = Array.isArray(payload.items) ? payload.items : [];
    elements.lastUpdated.textContent = computeLatestDate(latestItems);
    renderSelector();
    renderTable(latestItems);
    renderChart(latestItems);
    setStatus(dict.statusSynced || "");
  } catch (error) {
    console.error("Failed to load index history:", error);
    if (!latestItems.length) {
      elements.lastUpdated.textContent = "--";
      renderTable([]);
      if (elements.chartEmpty) {
        elements.chartEmpty.classList.remove("hidden");
        elements.chartEmpty.textContent = dict.loadFailed || "Failed to load data.";
      }
    }
    setStatus(error?.message || dict.loadFailed || "Failed to load data.", "error");
  } finally {
    if (shouldToggleButton) {
      setButtonLoading(false);
    }
  }
}

async function triggerManualSync() {
  if (!elements.refreshButton || elements.refreshButton.dataset.loading === "1") {
    return;
  }
  setButtonLoading(true);
  const dict = getDict();
  const payload = {};
  if (selectedCode) {
    payload.indexCodes = [selectedCode];
  }
  try {
    const response = await fetch(`${API_BASE}/control/sync/index-history`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      let message = response.statusText || `HTTP ${response.status}`;
      try {
        const details = await response.json();
        if (details?.detail) {
          message = details.detail;
        }
      } catch (parseError) {
        /* ignore */
      }
      if (response.status === 409) {
        setStatus(dict.statusRunning || message, "info");
        await loadIndexHistory(selectedCode, { force: true, skipButtonState: true });
        return;
      }
      throw new Error(message);
    }
    setStatus(dict.statusSyncing || "Syncing...", "info");
    await new Promise((resolve) => setTimeout(resolve, 1200));
    await loadIndexHistory(selectedCode, { force: true, skipButtonState: true });
  } catch (error) {
    console.error("Index history sync failed:", error);
    setStatus(error?.message || dict.statusFailed || dict.loadFailed || "Failed to load data.", "error");
    await loadIndexHistory(selectedCode, { force: true, skipButtonState: true });
  } finally {
    setButtonLoading(false);
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

  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => triggerManualSync());
  }
}

function initialize() {
  applyTranslations();
  bindEvents();
  loadIndexHistory(selectedCode, { force: true });
}

initialize();
