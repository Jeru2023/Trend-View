const translations = getTranslations("indicatorScreen");
const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const INDICATOR_CODE = "continuous_volume";

let currentLang = getInitialLanguage();
let currentItems = [];
let lastCapturedAt = null;
let isSyncing = false;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("indicator-refresh-btn"),
  status: document.getElementById("indicator-screen-status"),
  lastUpdated: document.getElementById("indicator-last-updated"),
  indicatorName: document.getElementById("indicator-name"),
  daysRange: document.getElementById("indicator-days-range"),
  volumeHint: document.getElementById("indicator-volume-hint"),
  tableBody: document.getElementById("indicator-table-body"),
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
  renderStatus(dict.statusIdle || "");
  renderSummary();
  renderTable();
  renderUpdated();
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
  const dict = getDict();
  elements.indicatorName.textContent =
    dict.indicatorName || currentItems[0]?.indicatorName || "--";
  if (!currentItems.length) {
    elements.daysRange.textContent = "--";
    elements.volumeHint.textContent = "--";
    return;
  }
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
  elements.volumeHint.textContent =
    currentItems[0]?.volumeText || currentItems[0]?.volumeShares
      ? formatNumber(currentItems[0].volumeShares, { notation: "compact" })
      : "--";
}

function renderTable() {
  if (!elements.tableBody) {
    return;
  }
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
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.rank ?? "--"}</td>
      <td>${renderStockLink(item)}</td>
      <td>${renderStockLink(item, { useName: true })}</td>
      <td>${formatNumber(item.lastPrice, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
      <td class="${getTrendClass(item.priceChangePercent)}">${formatPercent(item.priceChangePercent)}</td>
      <td class="${getTrendClass(item.stageChangePercent)}">${formatPercent(item.stageChangePercent)}</td>
      <td>${item.volumeText || formatNumber(item.volumeShares, { notation: "compact" })}</td>
      <td>${item.baselineVolumeText || formatNumber(item.baselineVolumeShares, { notation: "compact" })}</td>
      <td>${item.volumeDays ?? "--"}</td>
      <td>${item.industry || "--"}</td>
    `;
    fragment.appendChild(row);
  });
  tbody.appendChild(fragment);
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

function getTrendClass(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return "";
  }
  return numeric > 0 ? "text-up" : "text-down";
}

async function fetchIndicatorData() {
  const dict = getDict();
  try {
    const response = await fetch(
      `${API_BASE}/indicator-screenings/continuous-volume?limit=500`
    );
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    lastCapturedAt = payload.capturedAt || null;
    renderStatus(dict.statusIdle || "");
    renderSummary();
    renderTable();
    renderUpdated();
  } catch (error) {
    console.error("Failed to load indicator screenings", error);
    renderStatus(dict.statusError || "Failed to load data.", "error");
    currentItems = [];
    lastCapturedAt = null;
    renderSummary();
    renderTable();
    renderUpdated();
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
    const response = await fetch(`${API_BASE}/indicator-screenings/continuous-volume/sync`, {
      method: "POST",
    });
    if (!response.ok) {
      throw new Error(`Sync failed with status ${response.status}`);
    }
    await response.json();
    renderStatus(dict.statusSuccess || "Updated.", "success");
    await fetchIndicatorData();
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
}

function initialize() {
  bindEvents();
  applyTranslations();
  fetchIndicatorData();
}

initialize();
