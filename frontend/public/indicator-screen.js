const translations = getTranslations("indicatorScreen");
const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const CONTINUOUS_VOLUME_CODE = "continuous_volume";
const VOLUME_PRICE_RISE_CODE = "volume_price_rise";
const UPWARD_BREAKOUT_CODE = "upward_breakout";
const CONTINUOUS_RISE_CODE = "continuous_rise";
const VOLUME_SURGE_BREAKOUT_CODE = "volume_surge_breakout";
const BIG_DEAL_INFLOW_CODE = "big_deal_inflow";
const PAGE_SIZE = 50;

const INDICATOR_OPTIONS = [
  {
    code: CONTINUOUS_VOLUME_CODE,
    label: { zh: "持续放量", en: "Continuous Volume" },
    columns: [
      { key: "volumeDays", labelKey: "colDays", type: "number" },
      { key: "stageChangePercent", labelKey: "colStageChange", type: "percent" },
      { key: "volumeText", labelKey: "colVolume", type: "text" },
    ],
    summaryExtraKey: "volumeText",
  },
  {
    code: VOLUME_PRICE_RISE_CODE,
    label: { zh: "量价齐升", en: "Volume & Price Rise" },
    columns: [
      { key: "volumeDays", labelKey: "colDays", type: "number" },
      { key: "stageChangePercent", labelKey: "colStageChange", type: "percent" },
      { key: "turnoverPercent", labelKey: "colTurnover", type: "percent" },
    ],
    summaryExtraKey: "turnoverPercent",
  },
  {
    code: UPWARD_BREAKOUT_CODE,
    label: { zh: "向上突破", en: "Upward Breakout" },
    columns: [
      { key: "priceChangePercent", labelKey: "colChange", type: "percent" },
      { key: "turnoverRate", labelKey: "colTurnoverRate", type: "percent" },
      { key: "volumeText", labelKey: "colVolume", type: "text" },
      { key: "turnoverAmountText", labelKey: "colAmount", type: "text" },
    ],
    summaryExtraKey: "turnoverRate",
  },
  {
    code: CONTINUOUS_RISE_CODE,
    label: { zh: "连续上涨", en: "Continuous Rise" },
    columns: [
      { key: "volumeDays", labelKey: "colDays", type: "number" },
      { key: "stageChangePercent", labelKey: "colStageChange", type: "percent" },
      { key: "turnoverPercent", labelKey: "colTurnover", type: "percent" },
      { key: "highPrice", labelKey: "colHigh", type: "number" },
      { key: "lowPrice", labelKey: "colLow", type: "number" },
    ],
    summaryExtraKey: "turnoverPercent",
  },
  {
    code: VOLUME_SURGE_BREAKOUT_CODE,
    label: { zh: "爆量启动", en: "Volume Spike Breakout" },
    columns: [
      { key: "priceChangePercent", labelKey: "colChange", type: "percent" },
      { key: "turnoverPercent", labelKey: "colVolumeMultiple", type: "number" },
      { key: "turnoverRate", labelKey: "colBreakout", type: "percent" },
      { key: "stageChangePercent", labelKey: "colStageChange", type: "percent" },
    ],
    summaryExtraKey: "volumeText",
  },
  {
    code: BIG_DEAL_INFLOW_CODE,
    label: { zh: "当日大单净流入", en: "Large Order Inflow" },
    columns: [
      { key: "bigDealNetAmount", labelKey: "colBigDealNet", type: "amount" },
      { key: "bigDealBuyAmount", labelKey: "colBigDealBuy", type: "amount" },
      { key: "bigDealSellAmount", labelKey: "colBigDealSell", type: "amount" },
      { key: "bigDealTradeCount", labelKey: "colBigDealTrades", type: "number" },
    ],
    summaryExtraKey: "bigDealNetAmount",
  },
];

let currentLang = getInitialLanguage();
let currentItems = [];
let lastCapturedAt = null;
let lastRealtimeSyncedAt = null;
let lastBigdealSyncedAt = null;
let isSyncing = false;
let isRealtimeUpdating = false;
let isBigdealUpdating = false;
let currentPage = 1;
let totalItems = 0;
let selectedIndicators = [CONTINUOUS_VOLUME_CODE];
let filters = {
  netIncomeYoyRatio: null,
  netIncomeQoqRatio: null,
  peMin: null,
  peMax: null,
  turnoverMin: null,
  turnoverMax: null,
  dailyChangeMin: null,
  dailyChangeMax: null,
  pctChange1WMax: null,
  pctChange1MMax: null,
};
let filterDebounceTimer = null;
const FILTER_DEBOUNCE_MS = 500;
let lastResponseIndicatorCodes = [];
let lastResponsePrimaryIndicator = null;
let latestSyncStats = {
  realtimeTrade: null,
  bigDeal: null,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("indicator-refresh-btn"),
  realtimeButton: document.getElementById("indicator-realtime-btn"),
  bigdealButton: document.getElementById("indicator-bigdeal-btn"),
  status: document.getElementById("indicator-screen-status"),
  lastUpdated: document.getElementById("indicator-last-updated"),
  realtimeUpdated: document.getElementById("indicator-realtime-updated"),
  bigdealUpdated: document.getElementById("indicator-bigdeal-updated"),
  indicatorTagContainer: document.getElementById("indicator-filter-tags"),
  tableHead: document.getElementById("indicator-table-head"),
  tableBody: document.getElementById("indicator-table-body"),
  resultCount: document.getElementById("indicator-result-count"),
  prevButton: document.getElementById("indicator-prev-btn"),
  nextButton: document.getElementById("indicator-next-btn"),
  pageLabel: document.getElementById("indicator-page-label"),
  tagList: document.getElementById("indicator-tag-list"),
  filterNetIncome: document.getElementById("filter-netincome"),
  filterNetIncomeQoq: document.getElementById("filter-netincome-qoq"),
  filterPeMin: document.getElementById("filter-pe-min"),
  filterPeMax: document.getElementById("filter-pe-max"),
  filterTurnoverMin: document.getElementById("filter-turnover-min"),
  filterTurnoverMax: document.getElementById("filter-turnover-max"),
  filterDailyChangeMin: document.getElementById("filter-daily-change-min"),
  filterDailyChangeMax: document.getElementById("filter-daily-change-max"),
  filterWeekChangeMax: document.getElementById("filter-week-change-max"),
  filterMonthChangeMax: document.getElementById("filter-month-change-max"),
  snapshotButton: document.getElementById("indicator-snapshot-btn"),
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
  return document.documentElement.lang && translations[document.documentElement.lang]
    ? document.documentElement.lang
    : "zh";
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
  return translations[currentLang] || translations.zh;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}%`;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (!Number.isNaN(date.getTime())) {
      const locale = currentLang === "zh" ? "zh-CN" : "en-US";
      return date.toLocaleString(locale, {
        year: "numeric",
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
  } catch (error) {
    /* no-op */
  }
  return String(value);
}

function escapeHTML(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeTsCode(value) {
  if (!value) {
    return "";
  }
  const text = String(value).trim().toUpperCase();
  if (!text) {
    return "";
  }
  if (text.includes(".")) {
    const [symbol, suffix] = text.split(".", 2);
    const normalizedSymbol = symbol ? symbol.trim().padStart(6, "0") : "";
    const normalizedSuffix = (suffix || "").trim().slice(0, 2) || "SH";
    return normalizedSymbol ? `${normalizedSymbol}.${normalizedSuffix}` : "";
  }
  const digits = text.replace(/[^0-9]/g, "").slice(-6);
  if (!digits) {
    return "";
  }
  const padded = digits.padStart(6, "0");
  let suffix = "SZ";
  if (["6", "9", "5"].includes(padded[0])) {
    suffix = "SH";
  } else if (["4", "8"].includes(padded[0])) {
    suffix = "BJ";
  }
  return `${padded}.${suffix}`;
}

function applyTranslations() {
  const dict = getDict();
  document.title = dict.title || document.title;
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    if (key && typeof dict[key] === "string") {
      el.textContent = dict[key];
    }
  });
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
  if (!isSyncing) {
    if (selectedIndicators.length > 1 && dict.statusIntersection) {
      const text = dict.statusIntersection.replace("{count}", selectedIndicators.length);
      renderStatus(text || dict.statusIdle || "");
    } else {
      renderStatus(dict.statusIdle || "");
    }
  }
  renderIndicatorTags();
  renderSummary();
  renderTable();
  renderUpdated();
  updatePaginationControls();
}

function renderStatus(message, type = "info") {
  if (!elements.status) {
    return;
  }
  elements.status.textContent = message || "";
  elements.status.className = `page-status page-status--${type}`;
}

function renderUpdated() {
  if (!elements.lastUpdated) {
    return;
  }
  elements.lastUpdated.textContent = lastCapturedAt ? formatDateTime(lastCapturedAt) : "--";
  renderRealtimeUpdated();
  renderBigdealUpdated();
}

function renderRealtimeUpdated() {
  if (!elements.realtimeUpdated) {
    return;
  }
  elements.realtimeUpdated.textContent = lastRealtimeSyncedAt ? formatDateTime(lastRealtimeSyncedAt) : "--";
}

function renderBigdealUpdated() {
  if (!elements.bigdealUpdated) {
    return;
  }
  elements.bigdealUpdated.textContent = lastBigdealSyncedAt ? formatDateTime(lastBigdealSyncedAt) : "--";
}

async function fetchSyncStats() {
  try {
    const response = await fetch(`${API_BASE}/indicator-screenings/stats`);
    if (!response.ok) {
      throw new Error(`Failed to load sync stats: ${response.status}`);
    }
    const data = await response.json();
    latestSyncStats = {
      realtimeTrade: data.realtimeTrade || {},
      bigDeal: data.bigDeal || {},
    };
    if (data.indicatorCapturedAt) {
      lastCapturedAt = data.indicatorCapturedAt;
      renderUpdated();
    }
    if (latestSyncStats.realtimeTrade?.finishedAt) {
      lastRealtimeSyncedAt = latestSyncStats.realtimeTrade.finishedAt;
      renderRealtimeUpdated();
    }
    if (latestSyncStats.bigDeal?.finishedAt) {
      lastBigdealSyncedAt = latestSyncStats.bigDeal.finishedAt;
      renderBigdealUpdated();
    }
    return data;
  } catch (error) {
    console.error("Failed to fetch indicator sync stats", error);
    return null;
  }
}

async function waitForJobCompletion(jobKey, timeoutMs = 180000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const stats = await fetchSyncStats();
    if (stats && stats[jobKey]) {
      const status = stats[jobKey].status;
      if (status && status !== "running") {
        return stats[jobKey];
      }
    }
    await delay(3000);
  }
  return null;
}

function renderSummary() {
  if (!elements.tagList) {
    return;
  }
  const labels = elements.tagList.querySelectorAll(".indicator-tag");
  labels.forEach((label) => {
    const code = label.dataset.indicatorCode;
    const isActive = selectedIndicators.includes(code);
    const tooltip = isActive ? buildIndicatorTooltip(code) : "";
    if (tooltip) {
      label.classList.add("has-tooltip");
      label.dataset.tooltip = tooltip;
      label.setAttribute("aria-label", tooltip.replace(/\n/g, ", "));
    } else {
      label.classList.remove("has-tooltip");
      delete label.dataset.tooltip;
      label.removeAttribute("aria-label");
    }
  });
}

function buildIndicatorTooltip(indicatorCode) {
  if (!indicatorCode) {
    return "";
  }
  const dict = getDict();
  const label = getIndicatorLabel(indicatorCode);
  const option = getIndicatorOption(indicatorCode) || {};
  const relatedItems = currentItems.filter((item) => {
    const codes = item.matchedIndicators || [];
    return codes.includes(indicatorCode) || item.indicatorCode === indicatorCode;
  });

  let daysText = "--";
  let volumeText = "--";

  if (relatedItems.length) {
    const detailsList = relatedItems.map((item) => getIndicatorData(item, indicatorCode));
    const validDays = detailsList
      .map((detail) => Number(detail?.volumeDays ?? detail?.volume_days))
      .filter((value) => Number.isFinite(value));
    if (validDays.length) {
      const min = Math.min(...validDays);
      const max = Math.max(...validDays);
      daysText = min === max ? `${min}` : `${min} ~ ${max}`;
    }

    const extraKey = option.summaryExtraKey || "volumeText";
    if (extraKey === "turnoverPercent" || extraKey === "turnoverRate") {
      const values = detailsList
        .map((detail) => Number(detail?.[extraKey]))
        .filter((value) => Number.isFinite(value));
      if (values.length) {
        const min = Math.min(...values).toFixed(2);
        const max = Math.max(...values).toFixed(2);
        volumeText = min === max ? `${min}%` : `${min}% ~ ${max}%`;
      }
    } else if (extraKey === "bigDealNetAmount") {
      const values = detailsList
        .map((detail) => Number(detail?.bigDealNetAmount))
        .filter((value) => Number.isFinite(value));
      if (values.length) {
        const min = Math.min(...values);
        const max = Math.max(...values);
        const minText = formatNumber(min, { notation: "compact" });
        const maxText = formatNumber(max, { notation: "compact" });
        volumeText = min === max ? minText : `${minText} ~ ${maxText}`;
      }
    } else if (extraKey === "volumeText") {
      const value =
        detailsList.find((detail) => typeof detail?.volumeText === "string")?.volumeText ??
        detailsList.find((detail) => Number.isFinite(detail?.volumeShares))?.volumeShares;
      if (typeof value === "string") {
        volumeText = value;
      } else if (Number.isFinite(value)) {
        volumeText = formatNumber(value, { notation: "compact" });
      }
    } else {
      const value = detailsList.find((detail) => detail?.[extraKey] != null)?.[extraKey];
      if (value != null) {
        volumeText = String(value);
      }
    }
  }

  const lines = [
    `${dict.tableIndicator || "Indicator"}: ${label || "--"}`,
    `${dict.colDays || "Days"}: ${daysText}`,
    `${dict.colVolume || "Volume"}: ${volumeText}`,
  ];
  if (!relatedItems.length) {
    lines.push(dict.tooltipNoData || "Load this indicator to view parameters.");
  }
  return lines.join("\n");
}

function renderTable() {
  if (!elements.tableBody) {
    return;
  }
  const columns = getColumnsForSelection();
  const activeIndicator = primaryIndicator();
  renderTableHead(columns);

  const tbody = elements.tableBody;
  tbody.innerHTML = "";
  if (!currentItems.length) {
    const key = currentLang === "zh" ? "empty-zh" : "empty-en";
    const emptyText = tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = columns.length || 1;
    cell.className = "table-empty";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  const fragment = document.createDocumentFragment();
  currentItems.forEach((item) => {
    const indicatorData = getIndicatorData(item, activeIndicator);
    const row = document.createElement("tr");
    const cells = columns.map((column) => {
      return `<td>${column.renderer(item, indicatorData)}</td>`;
    });
    row.innerHTML = cells.join("");
    fragment.appendChild(row);
  });
  tbody.appendChild(fragment);
}

function renderTableHead(columns) {
  if (!elements.tableHead) {
    return;
  }
  const dict = getDict();
  elements.tableHead.innerHTML = columns
    .map((column) => `<th>${dict[column.labelKey] || ""}</th>`)
    .join("");
}

function renderStockLink(item, options = {}) {
  const code = item?.stockCodeFull || item?.stockCode;
  const text = options.useName ? item?.stockName || "--" : code || "--";
  if (!code || text === "--") {
    return "--";
  }
  const url = `stock-detail.html?code=${encodeURIComponent(code)}`;
  return `<a href="${url}" target="_blank" rel="noopener">${escapeHTML(text)}</a>`;
}

function renderStockCell(record) {
  const codeLink = renderStockLink(record);
  const nameLink = renderStockLink(record, { useName: true });
  if (codeLink === "--" && nameLink === "--") {
    return "--";
  }
  const nameLine = nameLink === "--" ? "" : `<div class="stock-cell__name">${nameLink}</div>`;
  return `<div class="stock-cell"><div class="stock-cell__code">${codeLink}</div>${nameLine}</div>`;
}

function renderIndicatorBadges(record) {
  if (!Array.isArray(record.matchedIndicators) || !record.matchedIndicators.length) {
    return "--";
  }
  return `<span class="indicator-badges">${record.matchedIndicators
    .map((code) => `<span class="indicator-badge">${escapeHTML(getIndicatorLabel(code))}</span>`)
    .join("")}</span>`;
}

function renderBigDealCell(record) {
  const dict = getDict();
  if (record.hasBigDealInflow) {
    return `<span class="text-up">${dict.bigDealYes || "Yes"}</span>`;
  }
  return dict.bigDealNo || "--";
}

function getTrendClass(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return "";
  }
  return numeric > 0 ? "text-up" : "text-down";
}

async function fetchIndicatorData(page = currentPage) {
  const dict = getDict();
  try {
    const limit = PAGE_SIZE;
    const offset = (page - 1) * PAGE_SIZE;
    const params = new URLSearchParams();
    params.set("limit", limit);
    params.set("offset", offset);
    selectedIndicators.forEach((code) => params.append("indicators", code));
    if (filters.netIncomeYoyRatio !== null) {
      params.set("netIncomeYoyMin", String(filters.netIncomeYoyRatio));
    }
    if (filters.netIncomeQoqRatio !== null) {
      params.set("netIncomeQoqMin", String(filters.netIncomeQoqRatio));
    }
    if (filters.peMin !== null) {
      params.set("peMin", String(filters.peMin));
    }
    if (filters.peMax !== null) {
      params.set("peMax", String(filters.peMax));
    }
    if (filters.turnoverMin !== null) {
      params.set("turnoverRateMin", String(filters.turnoverMin));
    }
    if (filters.turnoverMax !== null) {
      params.set("turnoverRateMax", String(filters.turnoverMax));
    }
    if (filters.dailyChangeMin !== null) {
      params.set("dailyChangeMin", String(filters.dailyChangeMin));
    }
    if (filters.dailyChangeMax !== null) {
      params.set("dailyChangeMax", String(filters.dailyChangeMax));
    }
    if (filters.pctChange1WMax !== null) {
      params.set("pctChange1WMax", String(filters.pctChange1WMax));
    }
    if (filters.pctChange1MMax !== null) {
      params.set("pctChange1MMax", String(filters.pctChange1MMax));
    }
    const response = await fetch(`${API_BASE}/indicator-screenings?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    lastCapturedAt = payload.capturedAt || null;
    lastResponseIndicatorCodes = Array.isArray(payload.indicatorCodes) ? payload.indicatorCodes : [];
    lastResponsePrimaryIndicator =
      payload.indicatorCode || lastResponseIndicatorCodes[0] || lastResponsePrimaryIndicator;
    totalItems = Number(payload.total) || 0;
    currentPage = page;
    if (elements.snapshotButton) {
      elements.snapshotButton.disabled = currentItems.length === 0;
    }
    renderStatus(dict.statusIdle || "");
    renderSummary();
    renderTable();
    renderUpdated();
    updatePaginationControls();
  } catch (error) {
    console.error("Failed to load indicator screenings", error);
    renderStatus(dict.statusError || "Failed to load data.", "error");
    currentItems = [];
    lastCapturedAt = null;
    totalItems = 0;
    lastResponseIndicatorCodes = [];
    lastResponsePrimaryIndicator = null;
    if (elements.snapshotButton) {
      elements.snapshotButton.disabled = true;
    }
    renderSummary();
    renderTable();
    renderUpdated();
    updatePaginationControls();
  }
}

async function handleRefresh() {
  if (isSyncing) {
    return;
  }
  const dict = getDict();
  isSyncing = true;
  if (elements.refreshButton) {
    elements.refreshButton.disabled = true;
  }
  renderStatus(dict.statusSyncing || "Updating…", "info");
  try {
    const response = await fetch(`${API_BASE}/indicator-screenings/sync`, {
      method: "POST",
    });
    if (!response.ok) {
      throw new Error(`Sync failed with status ${response.status}`);
    }
    const payload = await response.json();
    const results = Array.isArray(payload.results) ? payload.results : [];
    const updatedCount = results.filter((item) => !item.skipped).length;
    const skippedCount = results.filter((item) => item.skipped).length;
    const successTemplate =
      dict.statusSuccessDetailed || dict.statusSuccess || "Updated.";
    const message = successTemplate
      .replace("{updated}", updatedCount)
      .replace("{skipped}", skippedCount);
    renderStatus(message, "success");
    currentPage = 1;
    await fetchIndicatorData(1);
  } catch (error) {
    console.error("Failed to sync indicator screenings", error);
    renderStatus(dict.statusError || "Failed to refresh data.", "error");
  } finally {
    isSyncing = false;
    if (elements.refreshButton) {
      elements.refreshButton.disabled = false;
    }
  }
}

async function handleRealtimeRefresh() {
  if (isRealtimeUpdating) {
    return;
  }
  const dict = getDict();
  isRealtimeUpdating = true;
  if (elements.realtimeButton) {
    elements.realtimeButton.disabled = true;
  }
  renderStatus(dict.realtimeRunning || "Fetching realtime quotes…", "info");
  try {
    const response = await fetch(`${API_BASE}/indicator-screenings/realtime-refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ syncAll: true }),
    });
    if (!response.ok) {
      throw new Error(`Realtime refresh failed with status ${response.status}`);
    }
    const initialPayload = await response.json().catch(() => ({}));
    renderStatus(dict.realtimeQueued || dict.realtimeRunning || "Realtime job queued…", "info");
    const jobSnapshot = await waitForJobCompletion("realtimeTrade");
    const initialProcessed =
      initialPayload && typeof initialPayload.processed !== "undefined" ? initialPayload.processed : 0;
    const refreshedCount = Number(jobSnapshot?.totalRows ?? initialProcessed ?? 0);
    const successText = (dict.realtimeSuccess || "Realtime update completed ({count}).").replace(
      "{count}",
      refreshedCount,
    );
    renderStatus(successText, "success");
    await fetchIndicatorData(currentPage);
  } catch (error) {
    console.error("Failed to run realtime refresh", error);
    renderStatus(dict.realtimeError || "Realtime update failed.", "error");
  } finally {
    isRealtimeUpdating = false;
    if (elements.realtimeButton) {
      elements.realtimeButton.disabled = false;
    }
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
    elements.refreshButton.addEventListener("click", handleRefresh);
  }
  if (elements.realtimeButton) {
    elements.realtimeButton.addEventListener("click", handleRealtimeRefresh);
  }
  if (elements.bigdealButton) {
    elements.bigdealButton.addEventListener("click", handleBigdealRefresh);
  }
  if (elements.prevButton) {
    elements.prevButton.addEventListener("click", () => {
      if (currentPage > 1) {
        fetchIndicatorData(currentPage - 1);
      }
    });
  }
  if (elements.nextButton) {
    elements.nextButton.addEventListener("click", () => {
      const maxPage = Math.ceil(totalItems / PAGE_SIZE) || 1;
      if (currentPage < maxPage) {
        fetchIndicatorData(currentPage + 1);
      }
    });
  }
  if (elements.snapshotButton) {
    elements.snapshotButton.addEventListener("click", openSnapshotWindow);
  }
  if (elements.tagList) {
    elements.tagList.addEventListener("click", (event) => {
      const button = event.target.closest("[data-indicator-code]");
      if (!button) {
        return;
      }
      toggleIndicator(button.dataset.indicatorCode);
    });
  }
  if (elements.filterNetIncome) {
    elements.filterNetIncome.addEventListener("input", updateFilterState);
  }
  if (elements.filterNetIncomeQoq) {
    elements.filterNetIncomeQoq.addEventListener("input", updateFilterState);
  }
  if (elements.filterPeMin) {
    elements.filterPeMin.addEventListener("input", updateFilterState);
  }
  if (elements.filterPeMax) {
    elements.filterPeMax.addEventListener("input", updateFilterState);
  }
  if (elements.filterTurnoverMin) {
    elements.filterTurnoverMin.addEventListener("input", updateFilterState);
  }
  if (elements.filterTurnoverMax) {
    elements.filterTurnoverMax.addEventListener("input", updateFilterState);
  }
  if (elements.filterDailyChangeMin) {
    elements.filterDailyChangeMin.addEventListener("input", updateFilterState);
  }
  if (elements.filterDailyChangeMax) {
    elements.filterDailyChangeMax.addEventListener("input", updateFilterState);
  }
  if (elements.filterWeekChangeMax) {
    elements.filterWeekChangeMax.addEventListener("input", updateFilterState);
  }
  if (elements.filterMonthChangeMax) {
    elements.filterMonthChangeMax.addEventListener("input", updateFilterState);
  }
}

async function handleBigdealRefresh() {
  if (isBigdealUpdating) {
    return;
  }
  const dict = getDict();
  isBigdealUpdating = true;
  if (elements.bigdealButton) {
    elements.bigdealButton.disabled = true;
  }
  renderStatus(dict.bigdealRunning || "Updating big-deal data…", "info");
  try {
    const response = await fetch(`${API_BASE}/control/sync/big-deal-fund-flow`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      throw new Error(`Big-deal refresh failed with status ${response.status}`);
    }
    await response.json().catch(() => ({}));
    const queuedText = dict.bigdealQueued || dict.bigdealRunning || "Big-deal data sync queued.";
    renderStatus(queuedText, "info");
    await waitForJobCompletion("bigDeal");
    renderStatus(dict.bigdealSuccess || "Big-deal data sync completed.", "success");
  } catch (error) {
    console.error("Failed to sync big deal data", error);
    renderStatus(dict.bigdealError || "Big-deal data sync failed.", "error");
  } finally {
    isBigdealUpdating = false;
    if (elements.bigdealButton) {
      elements.bigdealButton.disabled = false;
    }
  }
}

function initialize() {
  bindEvents();
  applyTranslations();
  fetchIndicatorData();
  fetchSyncStats();
}

initialize();

function updatePaginationControls() {
  if (!elements.resultCount || !elements.pageLabel) {
    return;
  }
  const dict = getDict();
  const total = Number(totalItems) || 0;
  const offset = (currentPage - 1) * PAGE_SIZE;
  const start = total === 0 ? 0 : Math.min(total, offset + 1);
  const end = total === 0 ? 0 : Math.min(total, offset + currentItems.length);
  const resultTemplate = dict.resultCount || "Showing {start}-{end} of {total} results";
  elements.resultCount.textContent = resultTemplate
    .replace("{start}", start)
    .replace("{end}", end)
    .replace("{total}", total);
  const pageTemplate = dict.pageLabel || "Page {page}";
  elements.pageLabel.textContent = pageTemplate.replace("{page}", currentPage);
  const maxPage = Math.max(1, Math.ceil(total / PAGE_SIZE));
  if (elements.prevButton) {
    elements.prevButton.disabled = currentPage <= 1 || total === 0;
    elements.prevButton.textContent = dict.paginationPrev || "Prev";
  }
  if (elements.nextButton) {
    elements.nextButton.disabled = currentPage >= maxPage || total === 0;
    elements.nextButton.textContent = dict.paginationNext || "Next";
  }
}

function getIndicatorLabel(code) {
  const option = INDICATOR_OPTIONS.find((item) => item.code === code);
  if (!option) {
    return code;
  }
  return option.label[currentLang] || option.label.zh || code;
}

function renderIndicatorTags() {
  if (!elements.tagList) {
    return;
  }
  const fragment = document.createDocumentFragment();
  INDICATOR_OPTIONS.forEach((option) => {
    const label = document.createElement("label");
    label.className = `indicator-tag${
      selectedIndicators.includes(option.code) ? " indicator-tag--active" : ""
    }`;
    label.dataset.indicatorCode = option.code;
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selectedIndicators.includes(option.code);
    checkbox.setAttribute("aria-label", getIndicatorLabel(option.code));
    label.appendChild(checkbox);
    const span = document.createElement("span");
    span.textContent = getIndicatorLabel(option.code);
    label.appendChild(span);
    fragment.appendChild(label);
  });
  elements.tagList.innerHTML = "";
  elements.tagList.appendChild(fragment);
}

function resetIndicatorResults(message) {
  currentItems = [];
  totalItems = 0;
  lastCapturedAt = null;
  lastResponseIndicatorCodes = [];
  lastResponsePrimaryIndicator = null;
  if (elements.snapshotButton) {
    elements.snapshotButton.disabled = true;
  }
  renderIndicatorTags();
  renderSummary();
  renderTable();
  renderUpdated();
  updatePaginationControls();
  if (message) {
    renderStatus(message, "info");
  }
}

function toggleIndicator(code) {
  if (!code) {
    return;
  }
  if (selectedIndicators.includes(code)) {
    selectedIndicators = selectedIndicators.filter((item) => item !== code);
  } else {
    selectedIndicators = [...selectedIndicators, code];
  }
  renderIndicatorTags();
  currentPage = 1;
  fetchIndicatorData();
}

function openSnapshotWindow() {
  if (!currentItems.length) {
    return;
  }
  const snapshotPayload = currentItems.map((item) => ({
    code: item.stockCodeFull || item.stockCode,
    name: item.stockName || item.stockCode,
  }));
  const storageKey = `indicator_snapshot_${Date.now()}`;
  try {
    sessionStorage.setItem(storageKey, JSON.stringify(snapshotPayload));
  } catch (error) {
    console.error("Failed to cache snapshot payload", error);
    return;
  }
  const snapshotUrl = new URL("indicator-snapshot.html", window.location.href);
  snapshotUrl.searchParams.set("key", storageKey);
  window.open(snapshotUrl.toString(), "_blank");
}

function updateFilterState() {
  const percentValue = elements.filterNetIncome?.value ? Number(elements.filterNetIncome.value) : null;
  filters.netIncomeYoyRatio =
    percentValue !== null && Number.isFinite(percentValue) ? percentValue / 100 : null;
  const qoqValue = elements.filterNetIncomeQoq?.value ? Number(elements.filterNetIncomeQoq.value) : null;
  filters.netIncomeQoqRatio = qoqValue !== null && Number.isFinite(qoqValue) ? qoqValue / 100 : null;
  filters.peMin = elements.filterPeMin?.value ? Number(elements.filterPeMin.value) : null;
  filters.peMax = elements.filterPeMax?.value ? Number(elements.filterPeMax.value) : null;
  const turnoverMinValue = elements.filterTurnoverMin?.value ? Number(elements.filterTurnoverMin.value) : null;
  filters.turnoverMin = turnoverMinValue !== null && Number.isFinite(turnoverMinValue) ? turnoverMinValue : null;
  const turnoverMaxValue = elements.filterTurnoverMax?.value ? Number(elements.filterTurnoverMax.value) : null;
  filters.turnoverMax = turnoverMaxValue !== null && Number.isFinite(turnoverMaxValue) ? turnoverMaxValue : null;
  const dailyChangeMinValue = elements.filterDailyChangeMin?.value
    ? Number(elements.filterDailyChangeMin.value)
    : null;
  filters.dailyChangeMin =
    dailyChangeMinValue !== null && Number.isFinite(dailyChangeMinValue) ? dailyChangeMinValue : null;
  const dailyChangeMaxValue = elements.filterDailyChangeMax?.value
    ? Number(elements.filterDailyChangeMax.value)
    : null;
  filters.dailyChangeMax =
    dailyChangeMaxValue !== null && Number.isFinite(dailyChangeMaxValue) ? dailyChangeMaxValue : null;
  const weekChangeMaxValue = elements.filterWeekChangeMax?.value ? Number(elements.filterWeekChangeMax.value) : null;
  filters.pctChange1WMax =
    weekChangeMaxValue !== null && Number.isFinite(weekChangeMaxValue) ? weekChangeMaxValue : null;
  const monthChangeMaxValue = elements.filterMonthChangeMax?.value
    ? Number(elements.filterMonthChangeMax.value)
    : null;
  filters.pctChange1MMax =
    monthChangeMaxValue !== null && Number.isFinite(monthChangeMaxValue) ? monthChangeMaxValue : null;
  scheduleFilterRefresh();
}

function scheduleFilterRefresh() {
  if (filterDebounceTimer) {
    clearTimeout(filterDebounceTimer);
  }
  filterDebounceTimer = setTimeout(() => {
    currentPage = 1;
    fetchIndicatorData(1);
  }, FILTER_DEBOUNCE_MS);
}

function primaryIndicator() {
  if (selectedIndicators.length) {
    return selectedIndicators[0];
  }
  return lastResponsePrimaryIndicator || CONTINUOUS_VOLUME_CODE;
}

function getIndicatorOption(code) {
  return INDICATOR_OPTIONS.find((option) => option.code === code);
}

function getColumnsForSelection() {
  const option = getIndicatorOption(primaryIndicator());
  const indicatorColumns = (option?.columns || []).map((column, index) => ({
    id: `${option?.code || "indicator"}-${column.key}-${index}`,
    labelKey: column.labelKey,
    renderer: (record, detail) => formatColumnValue(detail?.[column.key], column.type),
  }));
  const indicatorIncludesPriceChange = indicatorColumns.some((col) => col.labelKey === "colChange");
  const includeBigDealColumns =
    selectedIndicators.includes(BIG_DEAL_INFLOW_CODE) && option?.code !== BIG_DEAL_INFLOW_CODE;
  const extraBigDealColumns = includeBigDealColumns
    ? [
        {
          id: "bigDealNetAmount",
          labelKey: "colBigDealNet",
          renderer: (record) => formatColumnValue(record.bigDealNetAmount, "amount"),
        },
        {
          id: "bigDealBuyAmount",
          labelKey: "colBigDealBuy",
          renderer: (record) => formatColumnValue(record.bigDealBuyAmount, "amount"),
        },
        {
          id: "bigDealSellAmount",
          labelKey: "colBigDealSell",
          renderer: (record) => formatColumnValue(record.bigDealSellAmount, "amount"),
        },
        {
          id: "bigDealTradeCount",
          labelKey: "colBigDealTrades",
          renderer: (record) => {
            const value = Number(record.bigDealTradeCount);
            return Number.isFinite(value) ? value : "--";
          },
        },
      ]
    : [];
  const baseColumns = [
    {
      id: "rank",
      labelKey: "colRank",
      renderer: (record) => record.rank ?? "--",
    },
    {
      id: "indicators",
      labelKey: "colIndicator",
      renderer: (record) => renderIndicatorBadges(record),
    },
    {
      id: "stock",
      labelKey: "colStock",
      renderer: (record) => renderStockCell(record),
    },
    {
      id: "price",
      labelKey: "colPrice",
      renderer: (record) =>
        formatNumber(record.lastPrice, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    },
  ];
  if (!indicatorIncludesPriceChange) {
    baseColumns.push({
      id: "pctChange",
      labelKey: "colChange",
      renderer: (record) => formatPercent(record.priceChangePercent),
    });
  }
  const tailColumns = [
    {
      id: "industry",
      labelKey: "colIndustry",
      renderer: (record) => record.industry || "--",
    },
    {
      id: "bigDeal",
      labelKey: "colBigDeal",
      renderer: (record) => renderBigDealCell(record),
    },
  ];
  return [...baseColumns, ...indicatorColumns, ...extraBigDealColumns, ...tailColumns];
}

function formatColumnValue(value, type) {
  if (type === "percent") {
    return formatPercent(value);
  }
  if (type === "number") {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : "--";
  }
  if (type === "amount") {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return "--";
    }
    return formatNumber(numeric, { notation: "compact", maximumFractionDigits: 1 });
  }
  if (type === "text") {
    return value ? escapeHTML(String(value)) : "--";
  }
  return value ?? "--";
}

function getIndicatorData(record, indicatorCode) {
  if (record.indicatorDetails && record.indicatorDetails[indicatorCode]) {
    return record.indicatorDetails[indicatorCode];
  }
  return record;
}
