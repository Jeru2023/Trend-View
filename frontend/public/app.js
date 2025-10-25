const translations = getTranslations("basicInfo");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const PAGE_SIZE = 20;

const exchangeLabels = {
  en: { SSE: "SSE", SZSE: "SZSE", BSE: "BSE" },
  zh: { SSE: "上交所", SZSE: "深交所", BSE: "北交所" },
};

const marketLabels = {
  en: {
    主板: "Main Board",
    创业板: "ChiNext",
    科创板: "STAR Market",
    "Main Board": "Main Board",
    "ChiNext": "ChiNext",
    "STAR Market": "STAR Market",
  },
  zh: {
    主板: "主板",
    创业板: "创业板",
    科创板: "科创板",
    "Main Board": "主板",
    "ChiNext": "创业板",
    "STAR Market": "科创板",
  },
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
  return browserLang.startsWith("zh") ? "zh" : "en";
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
const state = {
  page: 1,
  total: 0,
  items: [],
  filters: {
    keyword: "",
    market: "all",
    exchange: "all",
  },
};

const elements = {
  fundamentalsBody: document.getElementById("fundamentals-body"),
  fundamentalsReportsBody: document.getElementById("fundamentals-reports-body"),
  statisticsBody: document.getElementById("statistics-body"),
  tabs: document.querySelectorAll(".tab"),
  tables: {
    fundamentals: document.getElementById("fundamentals-table"),
    fundamentalsReports: document.getElementById("fundamentals-reports-table"),
    statistics: document.getElementById("statistics-table"),
  },
  langButtons: document.querySelectorAll(".lang-btn"),
  searchBox: document.querySelector(".search-box"),
  keywordInput: document.getElementById("keyword"),
  marketSelect: document.getElementById("market"),
  exchangeSelect: document.getElementById("exchange"),
  applyButton: document.getElementById("apply-filters"),
  resetButton: document.getElementById("reset-filters"),
  prevPage: document.getElementById("prev-page"),
  nextPage: document.getElementById("next-page"),
  pageInfo: document.getElementById("page-info"),
};

const EMPTY_VALUE = "--";

function formatNumber(value) {
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 2 }).format(
    value ?? 0
  );
}

function formatOptionalNumber(value, options = {}) {
  if (value === null || value === undefined) {
    return EMPTY_VALUE;
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(value);
}

function formatOptionalDate(value) {
  if (!value) {
    return EMPTY_VALUE;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toISOString().slice(0, 10);
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.title = dict.title;

  document
    .querySelectorAll("[data-i18n]")
    .forEach((el) => {
      const key = el.dataset.i18n;
      const value = dict[key];
      if (typeof value === "string") {
        el.textContent = value;
      }
    });

  document
    .querySelectorAll("[data-placeholder-en]")
    .forEach((el) => {
      const placeholder = el.dataset[`placeholder${currentLang.toUpperCase()}`];
      if (typeof placeholder === "string") {
        el.placeholder = placeholder;
      }
    });
}

function formatPercent(value, { fromRatio = false } = {}) {
  if (value === null || value === undefined) {
    return EMPTY_VALUE;
  }
  const percentValue = fromRatio ? value * 100 : value;
  const formatted = percentValue.toFixed(2);
  return `${percentValue >= 0 ? "+" : ""}${formatted}%`;
}

function formatFinancialPercent(value) {
  if (value === null || value === undefined) {
    return EMPTY_VALUE;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return EMPTY_VALUE;
  }
  const treatAsRatio = Math.abs(numeric) <= 1;
  return formatPercent(numeric, { fromRatio: treatAsRatio });
}

function getTrendClass(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return value >= 0 ? "text-up" : "text-down";
}

function appendEmptyRow(body, colSpan) {
  if (!body) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = colSpan;
  cell.textContent = translations[currentLang].noData;
  cell.style.textAlign = "center";
  cell.style.color = "#6b7280";
  row.appendChild(cell);
  body.appendChild(row);
}

function renderTable(data = state.items) {
  elements.fundamentalsBody.innerHTML = "";
  if (elements.fundamentalsReportsBody) {
    elements.fundamentalsReportsBody.innerHTML = "";
  }
  if (elements.statisticsBody) {
    elements.statisticsBody.innerHTML = "";
  }

  if (!data.length) {
    appendEmptyRow(elements.fundamentalsBody, 11);
    if (elements.fundamentalsReportsBody) {
      appendEmptyRow(elements.fundamentalsReportsBody, 14);
    }
    if (elements.statisticsBody) {
      appendEmptyRow(elements.statisticsBody, 11);
    }
    return;
  }

  const marketMap = marketLabels[currentLang] || {};
  const exchangeMap = exchangeLabels[currentLang] || {};

  data.forEach((item) => {
    const marketLabel = item.market ? marketMap[item.market] ?? item.market : EMPTY_VALUE;
    const exchangeLabel = item.exchange ? exchangeMap[item.exchange] ?? item.exchange : EMPTY_VALUE;
    const changeClass = getTrendClass(item.pct_change);
    const lastPrice = formatOptionalNumber(item.last_price, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const changeDisplay = formatPercent(item.pct_change);
    const volumeDisplay = formatOptionalNumber(item.volume, { maximumFractionDigits: 0 });
    const marketCapDisplay = formatOptionalNumber(item.market_cap, { maximumFractionDigits: 0 });
    const peDisplay = formatOptionalNumber(item.pe_ratio, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const turnoverDisplay =
      item.turnover_rate == null
        ? EMPTY_VALUE
        : `${formatOptionalNumber(item.turnover_rate, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}%`;

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.code}</td>
      <td>${item.name ?? EMPTY_VALUE}</td>
      <td>${item.industry ?? EMPTY_VALUE}</td>
      <td>${marketLabel}</td>
      <td>${exchangeLabel}</td>
      <td>${lastPrice}</td>
      <td class="${changeClass}">
        ${changeDisplay}
      </td>
      <td>${volumeDisplay}</td>
      <td>${marketCapDisplay}</td>
      <td>${peDisplay}</td>
      <td>${turnoverDisplay}</td>
    `;
    elements.fundamentalsBody.appendChild(row);
    
    if (elements.fundamentalsReportsBody) {
      const annDateDisplay = formatOptionalDate(item.ann_date);
      const endDateDisplay = formatOptionalDate(item.end_date);
      const basicEpsDisplay = formatOptionalNumber(item.basic_eps, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      const revenueDisplay = formatOptionalNumber(
        item.revenue === null || item.revenue === undefined
            ? null
            : item.revenue / 1_000_000,
        { maximumFractionDigits: 2 }
      );
      const operateProfitDisplay = formatOptionalNumber(
        item.operate_profit === null || item.operate_profit === undefined
            ? null
            : item.operate_profit / 1_000_000,
        { maximumFractionDigits: 2 }
      );
      const netIncomeDisplay = formatOptionalNumber(
        item.net_income === null || item.net_income === undefined
            ? null
            : item.net_income / 1_000_000,
        { maximumFractionDigits: 2 }
      );
      const grossMarginDisplay = formatOptionalNumber(
        item.gross_margin === null || item.gross_margin === undefined
            ? null
            : item.gross_margin / 1_000_000,
        { maximumFractionDigits: 2 }
      );
      const roeDisplay = formatFinancialPercent(item.roe);
      const netIncomeYoyLatestDisplay = formatPercent(item.net_income_yoy_latest, { fromRatio: true });
      const netIncomeYoyPrev1Display = formatPercent(item.net_income_yoy_prev1, { fromRatio: true });
      const netIncomeYoyPrev2Display = formatPercent(item.net_income_yoy_prev2, { fromRatio: true });
      const netIncomeQoqDisplay = formatPercent(item.net_income_qoq_latest, { fromRatio: true });
      const revenueYoyDisplay = formatPercent(item.revenue_yoy_latest, { fromRatio: true });
      const revenueQoqDisplay = formatPercent(item.revenue_qoq_latest, { fromRatio: true });
      const roeYoyDisplay = formatPercent(item.roe_yoy_latest, { fromRatio: true });
      const roeQoqDisplay = formatPercent(item.roe_qoq_latest, { fromRatio: true });
      
      const fundamentalsRow = document.createElement("tr");
      fundamentalsRow.innerHTML = `
        <td>${item.code}</td>
        <td>${item.name ?? EMPTY_VALUE}</td>
        <td>${annDateDisplay}</td>
        <td>${endDateDisplay}</td>
        <td>${basicEpsDisplay}</td>
        <td>${revenueDisplay}</td>
        <td>${operateProfitDisplay}</td>
        <td>${netIncomeDisplay}</td>
        <td>${grossMarginDisplay}</td>
        <td>${roeDisplay}</td>
        <td class="${getTrendClass(item.net_income_yoy_latest)}">${netIncomeYoyLatestDisplay}</td>
        <td class="${getTrendClass(item.net_income_yoy_prev1)}">${netIncomeYoyPrev1Display}</td>
        <td class="${getTrendClass(item.net_income_yoy_prev2)}">${netIncomeYoyPrev2Display}</td>
        <td class="${getTrendClass(item.net_income_qoq_latest)}">${netIncomeQoqDisplay}</td>
        <td class="${getTrendClass(item.revenue_yoy_latest)}">${revenueYoyDisplay}</td>
        <td class="${getTrendClass(item.revenue_qoq_latest)}">${revenueQoqDisplay}</td>
        <td class="${getTrendClass(item.roe_yoy_latest)}">${roeYoyDisplay}</td>
        <td class="${getTrendClass(item.roe_qoq_latest)}">${roeQoqDisplay}</td>
      `;
      elements.fundamentalsReportsBody.appendChild(fundamentalsRow);
    }
      
      if (elements.statisticsBody) {
      const statsValues = [
        { value: item.pct_change_1y },
        { value: item.pct_change_6m },
        { value: item.pct_change_3m },
        { value: item.pct_change_1m },
        { value: item.pct_change_2w },
        { value: item.pct_change_1w },
      ];
      const maValues = [item.ma_20, item.ma_10, item.ma_5];

      const statsRow = document.createElement("tr");
      statsRow.innerHTML = `
        <td>${item.code}</td>
        <td>${item.name ?? EMPTY_VALUE}</td>
        ${statsValues
          .map((entry) => {
            const display = formatPercent(entry.value, { fromRatio: true });
            const trendClass = getTrendClass(entry.value);
            return `<td class="${trendClass}">${display}</td>`;
          })
          .join("")}
        ${maValues
          .map((ma) =>
            `<td>${formatOptionalNumber(ma, {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })}</td>`
          )
          .join("")}
      `;
      elements.statisticsBody.appendChild(statsRow);
    }
  });
}

function collectFilters() {
  return {
    keyword: elements.keywordInput.value.trim(),
    market: elements.marketSelect.value,
    exchange: elements.exchangeSelect.value,
  };
}

function setActiveTab(tabName) {
  elements.tabs.forEach((tab) => {
    const isActive = tab.dataset.tab === tabName;
    tab.classList.toggle("tab--active", isActive);
  });

  Object.entries(elements.tables).forEach(([key, table]) => {
    if (!table) {
      return;
    }
    table.classList.toggle("hidden", key !== tabName);
  });

  if (tabName === "statistics" && !elements.tables.statistics) {
    console.warn("Statistics table is not available in the DOM.");
  }
}

function updateLanguage(lang) {
  persistLanguage(lang);
  currentLang = lang;
  elements.langButtons.forEach((btn) =>
    btn.classList.toggle("active", btn.dataset.lang === lang)
  );
  applyTranslations();
  renderTable();
  updatePaginationControls();
}

function updatePaginationControls() {
  const totalPages = Math.max(1, Math.ceil(state.total / PAGE_SIZE));
  const dict = translations[currentLang];
  const pageText = dict.paginationInfo
    .replace("{current}", state.page)
    .replace("{totalPages}", totalPages)
    .replace("{total}", formatNumber(state.total));
  elements.pageInfo.textContent = pageText;
  elements.prevPage.disabled = state.page <= 1;
  elements.nextPage.disabled = state.page >= totalPages;
}

async function loadStocks(page = 1) {
  state.page = page;
  const filters = collectFilters();
  state.filters = filters;

  const params = new URLSearchParams();
  params.set("limit", PAGE_SIZE.toString());
  params.set("offset", ((state.page - 1) * PAGE_SIZE).toString());

  if (filters.keyword) params.set("keyword", filters.keyword);
  if (filters.market && filters.market !== "all")
    params.set("market", filters.market);
  if (filters.exchange && filters.exchange !== "all")
    params.set("exchange", filters.exchange);

  try {
    const response = await fetch(`${API_BASE}/stocks?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    state.total = data.total;
    state.items = data.items.map((item) => ({
      code: item.code,
      name: item.name,
      industry: item.industry,
      market: item.market,
      exchange: item.exchange,
      last_price: item.lastPrice,
      pct_change: item.pctChange,
      volume: item.volume,
      trade_date: item.tradeDate,
      market_cap: item.marketCap,
      pe_ratio: item.peRatio,
      turnover_rate: item.turnoverRate,
      pct_change_1y: item.pctChange1Y,
      pct_change_6m: item.pctChange6M,
      pct_change_3m: item.pctChange3M,
      pct_change_1m: item.pctChange1M,
      pct_change_2w: item.pctChange2W,
      pct_change_1w: item.pctChange1W,
      ma_20: item.ma20,
      ma_10: item.ma10,
      ma_5: item.ma5,
      ann_date: item.annDate,
      end_date: item.endDate,
      basic_eps: item.basicEps,
      revenue: item.revenue,
      operate_profit: item.operateProfit,
      net_income: item.netIncome,
      gross_margin: item.grossMargin,
      roe: item.roe,
    }));
  } catch (error) {
    console.error("Failed to fetch stock data:", error);
    state.total = 0;
    state.items = [];
  }

  renderTable();
  updatePaginationControls();
}

elements.langButtons.forEach((btn) =>
  btn.addEventListener("click", () => updateLanguage(btn.dataset.lang))
);

elements.applyButton.addEventListener("click", () => {
  loadStocks(1);
});

elements.resetButton.addEventListener("click", () => {
  elements.keywordInput.value = "";
  elements.marketSelect.value = "all";
  elements.exchangeSelect.value = "all";
  elements.searchBox.value = "";
  loadStocks(1);
});

elements.tabs.forEach((tab) =>
  tab.addEventListener("click", () => setActiveTab(tab.dataset.tab))
);

elements.prevPage.addEventListener("click", () => {
  if (state.page > 1) {
    loadStocks(state.page - 1);
  }
});

elements.nextPage.addEventListener("click", () => {
  const totalPages = Math.max(1, Math.ceil(state.total / PAGE_SIZE));
  if (state.page < totalPages) {
    loadStocks(state.page + 1);
  }
});

elements.searchBox.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    elements.keywordInput.value = event.target.value.trim();
    loadStocks(1);
  }
});

// Initial render
setActiveTab("fundamentals");
updateLanguage(currentLang);
loadStocks(1);





















