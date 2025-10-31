const translations = getTranslations("globalIndices");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("global-index-tbody"),
  lastSynced: document.getElementById("global-index-last-synced"),
  refreshButton: document.getElementById("global-index-refresh"),
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
}

function renderEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 11;
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
  if (!items.length) {
    const message =
      elements.tableBody.dataset[`empty${currentLang.toUpperCase()}`] || getDict().empty || "No data.";
    renderEmpty(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    const row = document.createElement("tr");
    const code = readValue(item, "code");
    const name = readValue(item, "name");
    const latestPrice = readValue(item, "latestPrice", "latest_price");
    const changeRaw = readValue(item, "changeAmount", "change_amount");
    const changePercentRaw = readValue(item, "changePercent", "change_percent");
    const openPrice = readValue(item, "openPrice", "open_price");
    const highPrice = readValue(item, "highPrice", "high_price");
    const lowPrice = readValue(item, "lowPrice", "low_price");
    const prevClose = readValue(item, "prevClose", "prev_close");
    const amplitude = readValue(item, "amplitude");
    const lastQuoteTime = readValue(item, "lastQuoteTime", "last_quote_time");

    const change = Number(changeRaw);
    const changePercent = Number(changePercentRaw);
    const up = change > 0;
    const down = change < 0;
    const changeClass = up ? "text-up" : down ? "text-down" : "";

    const cells = [
      code ?? "--",
      name ?? "--",
      formatNumber(latestPrice, { maximumFractionDigits: 2 }),
      changeClass
        ? `<span class="${changeClass}">${formatNumber(change, { maximumFractionDigits: 2 })}</span>`
        : formatNumber(change, { maximumFractionDigits: 2 }),
      changeClass ? `<span class="${changeClass}">${formatPercent(changePercent)}</span>` : formatPercent(changePercent),
      formatNumber(openPrice, { maximumFractionDigits: 2 }),
      formatNumber(highPrice, { maximumFractionDigits: 2 }),
      formatNumber(lowPrice, { maximumFractionDigits: 2 }),
      formatNumber(prevClose, { maximumFractionDigits: 2 }),
      formatPercent(amplitude),
      formatDateTime(lastQuoteTime),
    ];

    cells.forEach((value, index) => {
      const cell = document.createElement("td");
      if (index === 3 || index === 4) {
        cell.innerHTML = value;
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
  renderEmpty(dict.loading || "Loading...");

  try {
    const response = await fetch(`${API_BASE}/macro/global-indices`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    renderTable(data.items || []);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at);
  } catch (error) {
    console.error("Failed to fetch global indices:", error);
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
  }
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
    renderEmpty(error?.message || dict.loading || "Failed to load data");
    updateLastSynced(null);
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
