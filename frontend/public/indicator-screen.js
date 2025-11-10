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
];

let currentLang = getInitialLanguage();
let currentItems = [];
let lastCapturedAt = null;
let isSyncing = false;
let currentPage = 1;
let totalItems = 0;
let selectedIndicators = [CONTINUOUS_VOLUME_CODE];
let filters = {
  netIncomeYoyRatio: null,
  netIncomeQoqRatio: null,
  peMin: null,
  peMax: null,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("indicator-refresh-btn"),
  status: document.getElementById("indicator-screen-status"),
  lastUpdated: document.getElementById("indicator-last-updated"),
  indicatorName: document.getElementById("indicator-name"),
  daysRange: document.getElementById("indicator-days-range"),
  volumeHint: document.getElementById("indicator-volume-hint"),
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
  filterApply: document.getElementById("indicator-filter-apply"),
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
}

function renderSummary() {
  if (!elements.indicatorName || !elements.daysRange || !elements.volumeHint) {
    return;
  }
  const label = selectedIndicators.map((code) => getIndicatorLabel(code)).join(" / ");
  elements.indicatorName.textContent = label || "--";
  if (!currentItems.length) {
    elements.daysRange.textContent = "--";
    elements.volumeHint.textContent = "--";
    return;
  }
  const option = getIndicatorOption(primaryIndicator()) || {};
  const validDays = currentItems
    .map((item) => Number(item.volumeDays))
    .filter((value) => Number.isFinite(value));
  if (validDays.length) {
    const min = Math.min(...validDays);
    const max = Math.max(...validDays);
    elements.daysRange.textContent = min === max ? `${min}` : `${min} ~ ${max}`;
  } else {
    elements.daysRange.textContent = "--";
  }
  const primaryDetails = getIndicatorData(currentItems[0], primaryIndicator());
  const extraKey = option.summaryExtraKey || "volumeText";
  if (extraKey === "turnoverPercent" || extraKey === "turnoverRate") {
    const values = currentItems
      .map((item) => Number(getIndicatorData(item, primaryIndicator())[extraKey]))
      .filter((value) => Number.isFinite(value));
    if (values.length) {
      const min = Math.min(...values).toFixed(2);
      const max = Math.max(...values).toFixed(2);
      elements.volumeHint.textContent = min === max ? `${min}%` : `${min}% ~ ${max}%`;
    } else {
      elements.volumeHint.textContent = "--";
    }
  } else if (extraKey === "volumeText") {
    elements.volumeHint.textContent =
      primaryDetails.volumeText ||
      (primaryDetails.volumeShares
        ? formatNumber(primaryDetails.volumeShares, { notation: "compact" })
        : "--");
  } else {
    const value = primaryDetails[extraKey];
    elements.volumeHint.textContent = value != null ? String(value) : "--";
  }
}

function renderTable() {
  if (!elements.tableBody) {
    return;
  }
  const columns = getColumnsForSelection();
  renderTableHead(columns);

  const tbody = elements.tableBody;
  tbody.innerHTML = "";
  if (!currentItems.length) {
    const key = currentLang === "zh" ? "empty-zh" : "empty-en";
    const emptyText = tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 10;
    cell.className = "table-empty";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  const fragment = document.createDocumentFragment();
  currentItems.forEach((item) => {
    const indicatorData = getIndicatorData(item, primaryIndicator());
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

function renderIndicatorBadges(record) {
  if (!Array.isArray(record.matchedIndicators) || !record.matchedIndicators.length) {
    return "--";
  }
  return `<span class="indicator-badges">${record.matchedIndicators
    .map((code) => `<span class="indicator-badge">${escapeHTML(getIndicatorLabel(code))}</span>`)
    .join("")}</span>`;
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
    const response = await fetch(`${API_BASE}/indicator-screenings?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    lastCapturedAt = payload.capturedAt || null;
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
    elements.filterNetIncome.addEventListener("change", updateFilterState);
  }
  if (elements.filterPeMin) {
    elements.filterPeMin.addEventListener("change", updateFilterState);
  }
  if (elements.filterPeMax) {
    elements.filterPeMax.addEventListener("change", updateFilterState);
  }
  if (elements.filterApply) {
    elements.filterApply.addEventListener("click", handleFilterApply);
  }
}

function initialize() {
  bindEvents();
  applyTranslations();
    fetchIndicatorData();
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

function toggleIndicator(code) {
  if (!code) {
    return;
  }
  let shouldFetch = true;
  if (selectedIndicators.includes(code)) {
    selectedIndicators = selectedIndicators.filter((item) => item !== code);
    if (selectedIndicators.length === 0) {
      shouldFetch = false;
      currentItems = [];
      totalItems = 0;
      lastCapturedAt = null;
      renderIndicatorTags();
      renderSummary();
      renderTable();
      renderUpdated();
      updatePaginationControls();
      const dict = getDict();
      renderStatus(dict.statusNeedIndicator || "Select at least one indicator.", "info");
    }
  } else {
    selectedIndicators = [...selectedIndicators, code];
  }
  if (!shouldFetch) {
    return;
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
}

function handleFilterApply() {
  updateFilterState();
  currentPage = 1;
  fetchIndicatorData(1);
}

function primaryIndicator() {
  return selectedIndicators[0] || CONTINUOUS_VOLUME_CODE;
}

function getIndicatorOption(code) {
  return INDICATOR_OPTIONS.find((option) => option.code === code);
}

function getColumnsForSelection() {
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
      id: "code",
      labelKey: "colCode",
      renderer: (record) => renderStockLink(record),
    },
    {
      id: "name",
      labelKey: "colName",
      renderer: (record) => renderStockLink(record, { useName: true }),
    },
    {
      id: "price",
      labelKey: "colPrice",
      renderer: (record) =>
        formatNumber(record.lastPrice, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    },
  ];
  const option = getIndicatorOption(primaryIndicator());
  const indicatorColumns = (option?.columns || []).map((column, index) => ({
    id: `${option?.code || "indicator"}-${column.key}-${index}`,
    labelKey: column.labelKey,
    renderer: (record, detail) => formatColumnValue(detail?.[column.key], column.type),
  }));
  const tailColumns = [
    {
      id: "industry",
      labelKey: "colIndustry",
      renderer: (record) => record.industry || "--",
    },
  ];
  return [...baseColumns, ...indicatorColumns, ...tailColumns];
}

function formatColumnValue(value, type) {
  if (type === "percent") {
    return formatPercent(value);
  }
  if (type === "number") {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : "--";
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
