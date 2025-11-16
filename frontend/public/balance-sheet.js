const translations = getTranslations("balanceSheet");
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
  searchInput: document.getElementById("balance-sheet-search"),
  searchButton: document.getElementById("balance-sheet-search-btn"),
  tableBody: document.getElementById("balance-sheet-tbody"),
  prevButton: document.getElementById("balance-sheet-prev"),
  nextButton: document.getElementById("balance-sheet-next"),
  pageInfo: document.getElementById("balance-sheet-page-info"),
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
  document.title = dict.balanceSheetTitle || "Trend View - Balance Sheet Statements";

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

async function fetchBalanceSheets() {
  const params = buildQuery();
  const response = await fetch(`${API_BASE}/financial/balance-sheet?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data = await response.json();
  STATE.total = data.total || 0;
  STATE.items = Array.isArray(data.items) ? data.items : [];
}

function renderTable() {
  const tbody = elements.tableBody;
  const dict = translations[currentLang];
  tbody.innerHTML = "";
  if (!STATE.items.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 6;
    const emptyText = tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  STATE.items.forEach((item) => {
    const row = document.createElement("tr");
    const code = item.tsCode || item.ts_code || "--";
    const companyCell = `<div class="cell-main">
      <div>${escapeHTML(item.name || "--")}</div>
      <div class="cell-sub">${escapeHTML(code)}</div>
    </div>`;
    const reportingCell = `<div class="cell-main">
      <div>${formatDate(item.endDate || item.end_date)}</div>
      <div class="cell-sub">${dict.announced || "Announced"}: ${formatDate(item.annDate || item.ann_date)}</div>
    </div>`;

    const assetsCell = `<div class="cell-metric">
      <span class="cell-metric__title">${dict.sectionLiquidity || "Liquidity"}</span>
      <div>${dict.labelMoneyCap || "Cash"}: <strong>${formatCurrency(item.moneyCap)}</strong></div>
      <div>${dict.labelAccountsReceiv || "Accounts Receivable"}: <strong>${formatCurrency(item.accountsReceiv)}</strong></div>
      <div>${dict.labelInventories || "Inventories"}: <strong>${formatCurrency(item.inventories)}</strong></div>
      <div>${dict.labelFixAssets || "Fixed Assets"}: <strong>${formatCurrency(item.fixAssets)}</strong></div>
    </div>`;

    const totalsCell = `<div class="cell-metric">
      <span class="cell-metric__title">${dict.sectionAssetTotals || "Asset Totals"}</span>
      <div>${dict.labelTotalCurAssets || "Total Current Assets"}: <strong>${formatCurrency(item.totalCurAssets)}</strong></div>
      <div>${dict.labelTotalNca || "Total Non-Current Assets"}: <strong>${formatCurrency(item.totalNca)}</strong></div>
      <div>${dict.labelTotalAssets || "Total Assets"}: <strong>${formatCurrency(item.totalAssets)}</strong></div>
    </div>`;

    const liabilitiesCell = `<div class="cell-metric">
      <span class="cell-metric__title">${dict.sectionLiabilities || "Liabilities"}</span>
      <div>${dict.labelStBorr || "Short-term Loans"}: <strong>${formatCurrency(item.stBorr)}</strong></div>
      <div>${dict.labelLtBorr || "Long-term Loans"}: <strong>${formatCurrency(item.ltBorr)}</strong></div>
      <div>${dict.labelAcctPayable || "Accounts Payable"}: <strong>${formatCurrency(item.acctPayable)}</strong></div>
      <div>${dict.labelTotalCurLiab || "Total Current Liabilities"}: <strong>${formatCurrency(item.totalCurLiab)}</strong></div>
      <div>${dict.labelTotalNcl || "Total Non-Current Liabilities"}: <strong>${formatCurrency(item.totalNcl)}</strong></div>
      <div>${dict.labelTotalLiab || "Total Liabilities"}: <strong>${formatCurrency(item.totalLiab)}</strong></div>
    </div>`;

    const equityCell = `<div class="cell-metric">
      <span class="cell-metric__title">${dict.sectionEquity || "Equity"}</span>
      <div>${dict.labelTotalShare || "Total Share Capital"}: <strong>${formatNumber(item.totalShare)}</strong></div>
      <div>${dict.labelCapRese || "Capital Reserve"}: <strong>${formatCurrency(item.capRese)}</strong></div>
      <div>${dict.labelSurplusRese || "Surplus Reserve"}: <strong>${formatCurrency(item.surplusRese)}</strong></div>
      <div>${dict.labelUndistributed || "Undistributed Profit"}: <strong>${formatCurrency(item.undistrPorfit)}</strong></div>
      <div>${dict.labelEquity || "Equity"}: <strong>${formatCurrency(item.totalHldrEqyExcMinInt)}</strong></div>
      <div>${dict.labelLiabEquity || "Liabilities + Equity"}: <strong>${formatCurrency(item.totalLiabHldrEqy)}</strong></div>
    </div>`;

    row.innerHTML = `
      <td>${companyCell}</td>
      <td>${reportingCell}</td>
      <td>${assetsCell}</td>
      <td>${totalsCell}</td>
      <td>${liabilitiesCell}</td>
      <td>${equityCell}</td>
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
    await fetchBalanceSheets();
    renderTable();
  } catch (error) {
    console.error("Failed to load balance sheet data:", error);
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
