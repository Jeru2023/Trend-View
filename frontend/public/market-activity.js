const translations = getTranslations("marketActivity");
const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
let currentItems = [];
let datasetTimestamp = null;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  summary: document.getElementById("market-activity-summary"),
  tableBody: document.getElementById("market-activity-tbody"),
  updated: document.getElementById("market-activity-updated"),
};

const SUMMARY_GROUPS = [
  { metrics: ["上涨"], labelKey: "summaryUp", hintKey: "summaryUpHint" },
  { metrics: ["下跌"], labelKey: "summaryDown", hintKey: "summaryDownHint" },
  { metrics: ["涨停", "真实涨停"], labelKey: "summaryLimitUp", hintKey: "summaryLimitUpHint" },
  { metrics: ["跌停", "真实跌停"], labelKey: "summaryLimitDown", hintKey: "summaryLimitDownHint" },
  { metrics: ["活跃度"], labelKey: "summaryActivity", hintKey: "summaryActivityHint" },
];

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

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (!Number.isNaN(date.getTime())) {
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

  renderSummary(currentItems);
  renderTable(currentItems);
  renderUpdated(datasetTimestamp);
}

function renderUpdated(timestamp) {
  if (!elements.updated) {
    return;
  }
  elements.updated.textContent = timestamp ? formatDateTime(timestamp) : "--";
}

function pickMetric(metrics, items) {
  for (const metricName of metrics) {
    const found = items.find((item) => item.metric === metricName);
    if (found) {
      return found;
    }
  }
  return undefined;
}

function renderSummary(items) {
  if (!elements.summary) {
    return;
  }
  const dict = getDict();
  const fragment = document.createDocumentFragment();

  SUMMARY_GROUPS.forEach((group) => {
    const data = pickMetric(group.metrics, items);
    if (!data) {
      return;
    }
    const card = document.createElement("div");
    card.className = "metric-card";

    const label = document.createElement("div");
    label.className = "metric-card__label";
    label.textContent = dict[group.labelKey] || group.metrics[0];

    const value = document.createElement("div");
    value.className = "metric-card__value";
    value.textContent =
      data.valueText ||
      formatNumber(data.valueNumber, { maximumFractionDigits: 2 }) ||
      "--";

    card.appendChild(label);
    card.appendChild(value);

    if (group.hintKey && dict[group.hintKey]) {
      const hint = document.createElement("div");
      hint.className = "metric-card__hint";
      hint.textContent = dict[group.hintKey];
      card.appendChild(hint);
    }

    fragment.appendChild(card);
  });

  elements.summary.innerHTML = "";
  elements.summary.appendChild(fragment);
}

function renderTable(items) {
  if (!elements.tableBody) {
    return;
  }
  if (!items.length) {
    const key = currentLang === "zh" ? "data-empty-zh" : "data-empty-en";
    const message =
      (elements.tableBody.dataset && elements.tableBody.dataset[key]) ||
      (currentLang === "zh" ? "暂无市场活跃度数据。" : "No market activity data.");
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 3;
    cell.className = "table-empty";
    cell.textContent = message;
    row.appendChild(cell);
    elements.tableBody.innerHTML = "";
    elements.tableBody.appendChild(row);
    return;
  }

  const fragment = document.createDocumentFragment();

  items.forEach((item) => {
    const row = document.createElement("tr");
    const numericText =
      item.valueNumber === null || item.valueNumber === undefined
        ? "--"
        : formatNumber(item.valueNumber, { maximumFractionDigits: 2 });

    const cells = [
      item.metric || "--",
      item.valueText || numericText || "--",
      numericText,
    ];

    cells.forEach((text) => {
      const cell = document.createElement("td");
      cell.textContent = text;
      row.appendChild(cell);
    });

    fragment.appendChild(row);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

async function loadMarketActivity() {
  try {
    const response = await fetch(`${API_BASE}/market/activity`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    datasetTimestamp = payload.datasetTimestamp || payload.dataset_timestamp || null;
    applyTranslations();
  } catch (error) {
    console.error("Failed to load market activity data", error);
    currentItems = [];
    datasetTimestamp = null;
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
}

function initialize() {
  bindEvents();
  applyTranslations();
  loadMarketActivity();
}

initialize();
