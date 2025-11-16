const translations = getTranslations("cashflow");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const STATE = {
  page: 1,
  limit: 20,
  total: 0,
  keyword: "",
  items: [],
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  searchInput: document.getElementById("cashflow-search"),
  searchButton: document.getElementById("cashflow-search-btn"),
  tableBody: document.getElementById("cashflow-tbody"),
  prevButton: document.getElementById("cashflow-prev"),
  nextButton: document.getElementById("cashflow-next"),
  pageInfo: document.getElementById("cashflow-page-info"),
};

const LANG_STORAGE_KEY = "trend-view-lang";

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
  return translations[browserLang] ? browserLang : "zh";
}

let currentLang = getInitialLanguage();

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* no-op */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
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

function formatNumber(value, fractionDigits = 2) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  }).format(numeric);
}

function formatCurrency(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    maximumFractionDigits: 1,
    notation: "compact",
  }).format(numeric);
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  try {
    if (value instanceof Date) {
      return value.toISOString().slice(0, 10);
    }
    const text = String(value).trim();
    if (/^\d{8}$/.test(text)) {
      return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
      return text;
    }
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString().slice(0, 10);
    }
  } catch (error) {
    /* ignore */
  }
  return String(value);
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = dict.cashflowTitle || "Trend View - Cash Flow Statements";

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const text = dict[key];
    if (typeof text === "string") {
      el.textContent = text;
    }
  });
  if (elements.searchInput) {
    const placeholderKey = elements.searchInput.dataset.i18nPlaceholder;
    if (placeholderKey && dict[placeholderKey]) {
      elements.searchInput.placeholder = dict[placeholderKey];
    }
  }
}

function buildQuery() {
  const params = new URLSearchParams();
  params.set("limit", String(STATE.limit));
  params.set("offset", String((STATE.page - 1) * STATE.limit));
  if (STATE.keyword) {
    params.set("keyword", STATE.keyword);
  }
  return params;
}

async function fetchCashflow() {
  const params = buildQuery();
  const response = await fetch(`${API_BASE}/financial/cashflow?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data = await response.json();
  STATE.total = data.total || 0;
  STATE.items = Array.isArray(data.items) ? data.items : [];
}

function renderTable() {
  const tbody = elements.tableBody;
  tbody.innerHTML = "";
  if (!STATE.items.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 7;
    const emptyText = tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  STATE.items.forEach((item) => {
    const row = document.createElement("tr");
    const code = item.tsCode || item.ts_code || "--";
    const nameCell = `<div class="cell-main">
      <div>${escapeHTML(item.name || "--")}</div>
      <div class="cell-sub">${escapeHTML(code)}</div>
    </div>`;
    const periodCell = `<div class="cell-main">
      <div>${formatDate(item.endDate || item.end_date)}</div>
      <div class="cell-sub" data-i18n="announced">${formatDate(item.annDate || item.ann_date)}</div>
    </div>`;

    const operatingCell = `<div class="cell-metric">
      <span data-i18n="labelReceived">${translations[currentLang].labelReceived || "Received"}</span>
      <strong>${formatCurrency(item.cFrSaleSg)}</strong>
      <span data-i18n="labelPaid">${translations[currentLang].labelPaid || "Paid"}</span>
      <strong>${formatCurrency(item.cPaidGoodsS)}</strong>
      <span data-i18n="labelNet">${translations[currentLang].labelNet || "Net"}</span>
      <strong>${formatCurrency(item.nCashflowAct)}</strong>
    </div>`;

    const investingCell = `<div class="cell-metric">
      <span data-i18n="labelCapex">${translations[currentLang].labelCapex || "Capex"}</span>
      <strong>${formatCurrency(item.cPayAcqConstFiolta)}</strong>
      <span data-i18n="labelNet">${translations[currentLang].labelNet || "Net"}</span>
      <strong>${formatCurrency(item.nCashflowInvAct)}</strong>
    </div>`;

    const financingCell = `<div class="cell-metric">
      <span data-i18n="labelBorrow">${translations[currentLang].labelBorrow || "Borrow"}</span>
      <strong>${formatCurrency(item.cRecpBorrow)}</strong>
      <span data-i18n="labelRepay">${translations[currentLang].labelRepay || "Repay"}</span>
      <strong>${formatCurrency(item.cPrepayAmtBorr)}</strong>
      <span data-i18n="labelNet">${translations[currentLang].labelNet || "Net"}</span>
      <strong>${formatCurrency(item.nCashFlowsFncAct)}</strong>
    </div>`;

    const netCell = `<div class="cell-metric">
      <span data-i18n="labelNetChange">${translations[currentLang].labelNetChange || "Net Change"}</span>
      <strong>${formatCurrency(item.nIncrCashCashEqu)}</strong>
      <span data-i18n="labelCashStart">${translations[currentLang].labelCashStart || "Start"}</span>
      <strong>${formatCurrency(item.cCashEquBegPeriod)}</strong>
      <span data-i18n="labelCashEnd">${translations[currentLang].labelCashEnd || "End"}</span>
      <strong>${formatCurrency(item.cCashEquEndPeriod)}</strong>
    </div>`;

    const fcfCell = `<strong>${formatCurrency(item.freeCashflow)}</strong>`;

    row.innerHTML = `
      <td>${nameCell}</td>
      <td>${periodCell}</td>
      <td>${operatingCell}</td>
      <td>${investingCell}</td>
      <td>${financingCell}</td>
      <td>${netCell}</td>
      <td>${fcfCell}</td>
    `;
    tbody.appendChild(row);
  });

  const totalPages = Math.max(1, Math.ceil(STATE.total / STATE.limit));
  elements.pageInfo.textContent = `${STATE.page} / ${totalPages}`;
  elements.prevButton.disabled = STATE.page <= 1;
  elements.nextButton.disabled = STATE.page >= totalPages;
}

async function loadAndRender() {
  try {
    await fetchCashflow();
    renderTable();
  } catch (error) {
    console.error("Failed to load cashflow data:", error);
  }
}

function handleLanguageSwitch(event) {
  const target = event.currentTarget;
  const lang = target?.dataset?.lang;
  if (!lang || !translations[lang]) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  applyTranslations();
  renderTable();
}

function handleSearch() {
  const text = elements.searchInput?.value?.trim() || "";
  STATE.keyword = text;
  STATE.page = 1;
  loadAndRender();
}

function bindEvents() {
  elements.langButtons.forEach((button) => {
    button.addEventListener("click", handleLanguageSwitch);
  });
  if (elements.searchInput) {
    elements.searchInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        handleSearch();
      }
    });
  }
  if (elements.searchButton) {
    elements.searchButton.addEventListener("click", handleSearch);
  }
  elements.prevButton.addEventListener("click", () => {
    if (STATE.page > 1) {
      STATE.page -= 1;
      loadAndRender();
    }
  });
  elements.nextButton.addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(STATE.total / STATE.limit));
    if (STATE.page < totalPages) {
      STATE.page += 1;
      loadAndRender();
    }
  });
}

applyTranslations();
bindEvents();
loadAndRender();
