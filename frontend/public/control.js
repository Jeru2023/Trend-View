const translations = getTranslations("controlPanel");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const INDUSTRY_FUND_FLOW_SYMBOLS = ["即时", "3日排行", "5日排行", "10日排行", "20日排行"];

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

let currentLang = getInitialLanguage();
let pollTimer = null;
let configState = {
  includeST: false,
  includeDelisted: false,
  dailyTradeWindowDays: 420,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  stockBasic: {
    status: document.getElementById("stock-basic-status"),
    updated: document.getElementById("stock-basic-updated"),
    duration: document.getElementById("stock-basic-duration"),
    rows: document.getElementById("stock-basic-rows"),
    message: document.getElementById("stock-basic-message"),
    progress: document.getElementById("stock-basic-progress"),
    button: document.getElementById("run-stock-basic"),
  },
  dailyTrade: {
    status: document.getElementById("daily-trade-status"),
    updated: document.getElementById("daily-trade-updated"),
    duration: document.getElementById("daily-trade-duration"),
    rows: document.getElementById("daily-trade-rows"),
    message: document.getElementById("daily-trade-message"),
    progress: document.getElementById("daily-trade-progress"),
    button: document.getElementById("run-daily-trade"),
  },
  dailyTradeMetrics: {
    status: document.getElementById("daily-trade-metrics-status"),
    updated: document.getElementById("daily-trade-metrics-updated"),
    duration: document.getElementById("daily-trade-metrics-duration"),
    rows: document.getElementById("daily-trade-metrics-rows"),
    message: document.getElementById("daily-trade-metrics-message"),
    progress: document.getElementById("daily-trade-metrics-progress"),
    button: document.getElementById("run-daily-trade-metrics"),
  },
  fundamentalMetrics: {
    status: document.getElementById("fundamental-metrics-status"),
    updated: document.getElementById("fundamental-metrics-updated"),
    duration: document.getElementById("fundamental-metrics-duration"),
    rows: document.getElementById("fundamental-metrics-rows"),
    message: document.getElementById("fundamental-metrics-message"),
    progress: document.getElementById("fundamental-metrics-progress"),
    button: document.getElementById("run-fundamental-metrics"),
  },
  peripheralAggregate: {
    status: document.getElementById("peripheral-aggregate-status"),
    updated: document.getElementById("peripheral-aggregate-updated"),
    duration: document.getElementById("peripheral-aggregate-duration"),
    rows: document.getElementById("peripheral-aggregate-rows"),
    message: document.getElementById("peripheral-aggregate-message"),
    progress: document.getElementById("peripheral-aggregate-progress"),
    button: document.getElementById("run-peripheral-aggregate"),
  },
  macroAggregate: {
    status: document.getElementById("macro-aggregate-status"),
    updated: document.getElementById("macro-aggregate-updated"),
    duration: document.getElementById("macro-aggregate-duration"),
    rows: document.getElementById("macro-aggregate-rows"),
    message: document.getElementById("macro-aggregate-message"),
    progress: document.getElementById("macro-aggregate-progress"),
    button: document.getElementById("run-macro-aggregate"),
  },
  fundFlowAggregate: {
    status: document.getElementById("fund-flow-aggregate-status"),
    updated: document.getElementById("fund-flow-aggregate-updated"),
    duration: document.getElementById("fund-flow-aggregate-duration"),
    rows: document.getElementById("fund-flow-aggregate-rows"),
    message: document.getElementById("fund-flow-aggregate-message"),
    progress: document.getElementById("fund-flow-aggregate-progress"),
    button: document.getElementById("run-fund-flow-aggregate"),
  },
  dailyIndicator: {
    status: document.getElementById("daily-indicator-status"),
    updated: document.getElementById("daily-indicator-updated"),
    duration: document.getElementById("daily-indicator-duration"),
    rows: document.getElementById("daily-indicator-rows"),
    message: document.getElementById("daily-indicator-message"),
    progress: document.getElementById("daily-indicator-progress"),
    button: document.getElementById("run-daily-indicator"),
  },
  realtimeTrade: {
    status: document.getElementById("realtime-trade-status"),
    updated: document.getElementById("realtime-trade-updated"),
    duration: document.getElementById("realtime-trade-duration"),
    rows: document.getElementById("realtime-trade-rows"),
    message: document.getElementById("realtime-trade-message"),
    progress: document.getElementById("realtime-trade-progress"),
    button: document.getElementById("run-realtime-trade"),
  },
  indexHistory: {
    status: document.getElementById("index-history-status"),
    updated: document.getElementById("index-history-updated"),
    duration: document.getElementById("index-history-duration"),
    rows: document.getElementById("index-history-rows"),
    message: document.getElementById("index-history-message"),
    progress: document.getElementById("index-history-progress"),
    button: document.getElementById("run-index-history"),
  },
  realtimeIndex: {
    status: document.getElementById("realtime-index-status"),
    updated: document.getElementById("realtime-index-updated"),
    duration: document.getElementById("realtime-index-duration"),
    rows: document.getElementById("realtime-index-rows"),
    message: document.getElementById("realtime-index-message"),
    progress: document.getElementById("realtime-index-progress"),
    button: document.getElementById("run-realtime-index"),
  },
  incomeStatement: {
    status: document.getElementById("income-statement-status"),
    updated: document.getElementById("income-statement-updated"),
    duration: document.getElementById("income-statement-duration"),
    rows: document.getElementById("income-statement-rows"),
    message: document.getElementById("income-statement-message"),
    progress: document.getElementById("income-statement-progress"),
    button: document.getElementById("run-income-statement"),
  },
  cashflowStatement: {
    status: document.getElementById("cashflow-statement-status"),
    updated: document.getElementById("cashflow-statement-updated"),
    duration: document.getElementById("cashflow-statement-duration"),
    rows: document.getElementById("cashflow-statement-rows"),
    message: document.getElementById("cashflow-statement-message"),
    progress: document.getElementById("cashflow-statement-progress"),
    button: document.getElementById("run-cashflow-statement"),
  },
  balanceSheet: {
    status: document.getElementById("balance-sheet-status"),
    updated: document.getElementById("balance-sheet-updated"),
    duration: document.getElementById("balance-sheet-duration"),
    rows: document.getElementById("balance-sheet-rows"),
    message: document.getElementById("balance-sheet-message"),
    progress: document.getElementById("balance-sheet-progress"),
    button: document.getElementById("run-balance-sheet"),
  },
  performanceExpress: {
    status: document.getElementById("performance-express-status"),
    updated: document.getElementById("performance-express-updated"),
    duration: document.getElementById("performance-express-duration"),
    rows: document.getElementById("performance-express-rows"),
    message: document.getElementById("performance-express-message"),
    progress: document.getElementById("performance-express-progress"),
    button: document.getElementById("run-performance-express"),
  },
  performanceForecast: {
    status: document.getElementById("performance-forecast-status"),
    updated: document.getElementById("performance-forecast-updated"),
    duration: document.getElementById("performance-forecast-duration"),
    rows: document.getElementById("performance-forecast-rows"),
    message: document.getElementById("performance-forecast-message"),
    progress: document.getElementById("performance-forecast-progress"),
    button: document.getElementById("run-performance-forecast"),
  },
  globalIndex: {
    status: document.getElementById("global-index-status"),
    updated: document.getElementById("global-index-updated"),
    duration: document.getElementById("global-index-duration"),
    rows: document.getElementById("global-index-rows"),
    message: document.getElementById("global-index-message"),
    progress: document.getElementById("global-index-progress"),
    button: document.getElementById("run-global-index"),
  },
  macroLeverage: {
    status: document.getElementById("macro-leverage-status"),
    updated: document.getElementById("macro-leverage-updated"),
    duration: document.getElementById("macro-leverage-duration"),
    rows: document.getElementById("macro-leverage-rows"),
    message: document.getElementById("macro-leverage-message"),
    progress: document.getElementById("macro-leverage-progress"),
    button: document.getElementById("run-macro-leverage"),
    infoButton: document.querySelector("[data-leverage-info]"),
  },
  macroInsight: {
    status: document.getElementById("macro-insight-status"),
    updated: document.getElementById("macro-insight-updated"),
    duration: document.getElementById("macro-insight-duration"),
    rows: document.getElementById("macro-insight-rows"),
    message: document.getElementById("macro-insight-message"),
    progress: document.getElementById("macro-insight-progress"),
    button: document.getElementById("run-macro-insight"),
  },
  peripheralInsight: {
    status: document.getElementById("peripheral-insight-status"),
    updated: document.getElementById("peripheral-insight-updated"),
    duration: document.getElementById("peripheral-insight-duration"),
    rows: document.getElementById("peripheral-insight-rows"),
    message: document.getElementById("peripheral-insight-message"),
    progress: document.getElementById("peripheral-insight-progress"),
    button: document.getElementById("run-peripheral-insight"),
  },
  socialFinancing: {
    status: document.getElementById("social-financing-status"),
    updated: document.getElementById("social-financing-updated"),
    duration: document.getElementById("social-financing-duration"),
    rows: document.getElementById("social-financing-rows"),
    message: document.getElementById("social-financing-message"),
    progress: document.getElementById("social-financing-progress"),
    button: document.getElementById("run-social-financing"),
    infoButton: document.querySelector("[data-social-financing-info]"),
  },
  cpiMonthly: {
    status: document.getElementById("cpi-status"),
    updated: document.getElementById("cpi-updated"),
    duration: document.getElementById("cpi-duration"),
    rows: document.getElementById("cpi-rows"),
    message: document.getElementById("cpi-message"),
    progress: document.getElementById("cpi-progress"),
    button: document.getElementById("run-cpi"),
    infoButton: document.querySelector("[data-cpi-info]"),
  },
  ppiMonthly: {
    status: document.getElementById("ppi-status"),
    updated: document.getElementById("ppi-updated"),
    duration: document.getElementById("ppi-duration"),
    rows: document.getElementById("ppi-rows"),
    message: document.getElementById("ppi-message"),
    progress: document.getElementById("ppi-progress"),
    button: document.getElementById("run-ppi"),
    infoButton: document.querySelector("[data-ppi-info]"),
  },
  lprRate: {
    status: document.getElementById("lpr-rate-status"),
    updated: document.getElementById("lpr-rate-updated"),
    duration: document.getElementById("lpr-rate-duration"),
    rows: document.getElementById("lpr-rate-rows"),
    message: document.getElementById("lpr-rate-message"),
    progress: document.getElementById("lpr-rate-progress"),
    button: document.getElementById("run-lpr-rate"),
    infoButton: document.querySelector("[data-lpr-rate-info]"),
  },
  shiborRate: {
    status: document.getElementById("shibor-rate-status"),
    updated: document.getElementById("shibor-rate-updated"),
    duration: document.getElementById("shibor-rate-duration"),
    rows: document.getElementById("shibor-rate-rows"),
    message: document.getElementById("shibor-rate-message"),
    progress: document.getElementById("shibor-rate-progress"),
    button: document.getElementById("run-shibor-rate"),
    infoButton: document.querySelector("[data-shibor-rate-info]"),
  },
  pmiMonthly: {
    status: document.getElementById("pmi-status"),
    updated: document.getElementById("pmi-updated"),
    duration: document.getElementById("pmi-duration"),
    rows: document.getElementById("pmi-rows"),
    message: document.getElementById("pmi-message"),
    progress: document.getElementById("pmi-progress"),
    button: document.getElementById("run-pmi"),
    infoButton: document.querySelector("[data-pmi-info]"),
  },
  m2Monthly: {
    status: document.getElementById("m2-status"),
    updated: document.getElementById("m2-updated"),
    duration: document.getElementById("m2-duration"),
    rows: document.getElementById("m2-rows"),
    message: document.getElementById("m2-message"),
    progress: document.getElementById("m2-progress"),
    button: document.getElementById("run-m2"),
    infoButton: document.querySelector("[data-m2-info]"),
  },
  dollarIndex: {
    status: document.getElementById("dollar-index-status"),
    updated: document.getElementById("dollar-index-updated"),
    duration: document.getElementById("dollar-index-duration"),
    rows: document.getElementById("dollar-index-rows"),
    message: document.getElementById("dollar-index-message"),
    progress: document.getElementById("dollar-index-progress"),
    button: document.getElementById("run-dollar-index"),
  },
  rmbMidpoint: {
    status: document.getElementById("rmb-midpoint-status"),
    updated: document.getElementById("rmb-midpoint-updated"),
    duration: document.getElementById("rmb-midpoint-duration"),
    rows: document.getElementById("rmb-midpoint-rows"),
    message: document.getElementById("rmb-midpoint-message"),
    progress: document.getElementById("rmb-midpoint-progress"),
    button: document.getElementById("run-rmb-midpoint"),
  },
  futuresRealtime: {
    status: document.getElementById("futures-realtime-status"),
    updated: document.getElementById("futures-realtime-updated"),
    duration: document.getElementById("futures-realtime-duration"),
    rows: document.getElementById("futures-realtime-rows"),
    message: document.getElementById("futures-realtime-message"),
    progress: document.getElementById("futures-realtime-progress"),
    button: document.getElementById("run-futures-realtime"),
  },
  fedStatements: {
    status: document.getElementById("fed-statements-status"),
    updated: document.getElementById("fed-statements-updated"),
    duration: document.getElementById("fed-statements-duration"),
    rows: document.getElementById("fed-statements-rows"),
    message: document.getElementById("fed-statements-message"),
    progress: document.getElementById("fed-statements-progress"),
    button: document.getElementById("run-fed-statements"),
  },
  profitForecast: {
    status: document.getElementById("profit-forecast-status"),
    updated: document.getElementById("profit-forecast-updated"),
    duration: document.getElementById("profit-forecast-duration"),
    rows: document.getElementById("profit-forecast-rows"),
    message: document.getElementById("profit-forecast-message"),
    progress: document.getElementById("profit-forecast-progress"),
    button: document.getElementById("run-profit-forecast"),
  },
  industryFundFlow: {
    status: document.getElementById("industry-fund-flow-status"),
    updated: document.getElementById("industry-fund-flow-updated"),
    duration: document.getElementById("industry-fund-flow-duration"),
    rows: document.getElementById("industry-fund-flow-rows"),
    message: document.getElementById("industry-fund-flow-message"),
    progress: document.getElementById("industry-fund-flow-progress"),
    button: document.getElementById("run-industry-fund-flow"),
  },
  marginAccount: {
    status: document.getElementById("margin-account-status"),
    updated: document.getElementById("margin-account-updated"),
    duration: document.getElementById("margin-account-duration"),
    rows: document.getElementById("margin-account-rows"),
    message: document.getElementById("margin-account-message"),
    progress: document.getElementById("margin-account-progress"),
    button: document.getElementById("run-margin-account"),
  },
  marketFundFlow: {
    status: document.getElementById("market-fund-flow-status"),
    updated: document.getElementById("market-fund-flow-updated"),
    duration: document.getElementById("market-fund-flow-duration"),
    rows: document.getElementById("market-fund-flow-rows"),
    message: document.getElementById("market-fund-flow-message"),
    progress: document.getElementById("market-fund-flow-progress"),
    button: document.getElementById("run-market-fund-flow"),
  },
  marketActivity: {
    status: document.getElementById("market-activity-status"),
    updated: document.getElementById("market-activity-updated"),
    duration: document.getElementById("market-activity-duration"),
    rows: document.getElementById("market-activity-rows"),
    message: document.getElementById("market-activity-message"),
    progress: document.getElementById("market-activity-progress"),
    button: document.getElementById("run-market-activity"),
  },
  conceptFundFlow: {
    status: document.getElementById("concept-fund-flow-status"),
    updated: document.getElementById("concept-fund-flow-updated"),
    duration: document.getElementById("concept-fund-flow-duration"),
    rows: document.getElementById("concept-fund-flow-rows"),
    message: document.getElementById("concept-fund-flow-message"),
    progress: document.getElementById("concept-fund-flow-progress"),
    button: document.getElementById("run-concept-fund-flow"),
  },
  conceptIndexHistory: {
    status: document.getElementById("concept-index-history-status"),
    updated: document.getElementById("concept-index-history-updated"),
    duration: document.getElementById("concept-index-history-duration"),
    rows: document.getElementById("concept-index-history-rows"),
    message: document.getElementById("concept-index-history-message"),
    progress: document.getElementById("concept-index-history-progress"),
    button: document.getElementById("run-concept-index-history"),
    conceptInput: document.getElementById("concept-index-history-concepts"),
    lookbackInput: document.getElementById("concept-index-history-lookback"),
    endDateInput: document.getElementById("concept-index-history-end-date"),
  },
  individualFundFlow: {
    status: document.getElementById("individual-fund-flow-status"),
    updated: document.getElementById("individual-fund-flow-updated"),
    duration: document.getElementById("individual-fund-flow-duration"),
    rows: document.getElementById("individual-fund-flow-rows"),
    message: document.getElementById("individual-fund-flow-message"),
    progress: document.getElementById("individual-fund-flow-progress"),
    button: document.getElementById("run-individual-fund-flow"),
  },
  bigDealFundFlow: {
    status: document.getElementById("big-deal-fund-flow-status"),
    updated: document.getElementById("big-deal-fund-flow-updated"),
    duration: document.getElementById("big-deal-fund-flow-duration"),
    rows: document.getElementById("big-deal-fund-flow-rows"),
    message: document.getElementById("big-deal-fund-flow-message"),
    progress: document.getElementById("big-deal-fund-flow-progress"),
    button: document.getElementById("run-big-deal-fund-flow"),
  },
  financialIndicator: {
    status: document.getElementById("financial-indicator-status"),
    updated: document.getElementById("financial-indicator-updated"),
    duration: document.getElementById("financial-indicator-duration"),
    rows: document.getElementById("financial-indicator-rows"),
    message: document.getElementById("financial-indicator-message"),
    progress: document.getElementById("financial-indicator-progress"),
    button: document.getElementById("run-financial-indicator"),
  },
  financeBreakfast: {
    status: document.getElementById("finance-breakfast-status"),
    updated: document.getElementById("finance-breakfast-updated"),
    duration: document.getElementById("finance-breakfast-duration"),
    rows: document.getElementById("finance-breakfast-rows"),
    message: document.getElementById("finance-breakfast-message"),
    progress: document.getElementById("finance-breakfast-progress"),
    button: document.getElementById("run-finance-breakfast"),
  },
  globalFlash: {
    status: document.getElementById("global-flash-status"),
    updated: document.getElementById("global-flash-updated"),
    duration: document.getElementById("global-flash-duration"),
    rows: document.getElementById("global-flash-rows"),
    message: document.getElementById("global-flash-message"),
    progress: document.getElementById("global-flash-progress"),
    button: document.getElementById("run-global-flash"),
  },
  globalFlashClassification: {
    status: document.getElementById("global-flash-classification-status"),
    updated: document.getElementById("global-flash-classification-updated"),
    duration: document.getElementById("global-flash-classification-duration"),
    rows: document.getElementById("global-flash-classification-rows"),
    message: document.getElementById("global-flash-classification-message"),
    progress: document.getElementById("global-flash-classification-progress"),
    button: document.getElementById("run-global-flash-classification"),
  },
  stockMainBusiness: {
    status: document.getElementById("stock-main-business-status"),
    updated: document.getElementById("stock-main-business-updated"),
    duration: document.getElementById("stock-main-business-duration"),
    rows: document.getElementById("stock-main-business-rows"),
    message: document.getElementById("stock-main-business-message"),
    progress: document.getElementById("stock-main-business-progress"),
    button: document.getElementById("run-stock-main-business"),
  },
  stockMainComposition: {
    status: document.getElementById("stock-main-composition-status"),
    updated: document.getElementById("stock-main-composition-updated"),
    duration: document.getElementById("stock-main-composition-duration"),
    rows: document.getElementById("stock-main-composition-rows"),
    message: document.getElementById("stock-main-composition-message"),
    progress: document.getElementById("stock-main-composition-progress"),
    button: document.getElementById("run-stock-main-composition"),
  },
  conceptDirectory: {
    status: document.getElementById("concept-directory-status"),
    updated: document.getElementById("concept-directory-updated"),
    duration: document.getElementById("concept-directory-duration"),
    rows: document.getElementById("concept-directory-rows"),
    message: document.getElementById("concept-directory-message"),
    progress: document.getElementById("concept-directory-progress"),
    button: document.getElementById("run-concept-directory"),
  },
  marketInsight: {
    status: document.getElementById("market-insight-status"),
    updated: document.getElementById("market-insight-updated"),
    duration: document.getElementById("market-insight-duration"),
    rows: document.getElementById("market-insight-rows"),
    message: document.getElementById("market-insight-message"),
    progress: document.getElementById("market-insight-progress"),
    button: document.getElementById("run-market-insight"),
  },
  industryInsight: {
    status: document.getElementById("industry-insight-status"),
    updated: document.getElementById("industry-insight-updated"),
    duration: document.getElementById("industry-insight-duration"),
    rows: document.getElementById("industry-insight-rows"),
    message: document.getElementById("industry-insight-message"),
    progress: document.getElementById("industry-insight-progress"),
    button: document.getElementById("run-industry-insight"),
  },
  conceptInsight: {
    status: document.getElementById("concept-insight-status"),
    updated: document.getElementById("concept-insight-updated"),
    duration: document.getElementById("concept-insight-duration"),
    rows: document.getElementById("concept-insight-rows"),
    message: document.getElementById("concept-insight-message"),
    progress: document.getElementById("concept-insight-progress"),
    button: document.getElementById("run-concept-insight"),
  },
};

function formatDateTime(value) {
  if (!value) return "--";
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  try {
    return new Date(value).toLocaleString(locale);
  } catch (err) {
    return value;
  }
}

function formatTradeDate(value) {
  if (!value) return "--";
  const str = String(value).trim();
  if (/^\d{8}$/.test(str)) {
    return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
  }
  return str;
}

function formatNumber(value) {
  if (value === null || value === undefined) return "--";
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale).format(num);
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "--";
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "--";
  if (value < 1) return `${(value * 1000).toFixed(0)} ms`;
  const mins = Math.floor(value / 60);
  const secs = value % 60;
  if (mins === 0) return `${secs.toFixed(1)} s`;
  return `${mins}m ${secs.toFixed(0)}s`;
}

function formatYmd(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return null;
  }
  const y = date.getFullYear();
  const m = `${date.getMonth() + 1}`.padStart(2, "0");
  const d = `${date.getDate()}`.padStart(2, "0");
  return `${y}${m}${d}`;
}

function parseDateInputValue(value) {
  if (!value) return null;
  const normalized = value.trim();
  if (!normalized) return null;
  const parsed = new Date(`${normalized}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function normalizeConceptList(raw) {
  if (!raw) return [];
  return Array.from(
    new Set(
      raw
        .split(/[\n,，、]/)
        .map((item) => item.trim())
        .filter(Boolean)
    )
  );
}

function setLang(lang) {
  persistLanguage(lang);
  currentLang = lang;
  elements.langButtons.forEach((btn) =>
    btn.classList.toggle("active", btn.dataset.lang === lang)
  );
  applyTranslations();
  loadStatus();
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
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

  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
    const key = el.dataset.i18nPlaceholder;
    const value = dict[key];
    if (typeof value === "string") {
      el.setAttribute("placeholder", value);
    }
  });

  if (elements.macroLeverage.infoButton) {
    const tooltip = dict.macroLeverageTooltip || "";
    elements.macroLeverage.infoButton.setAttribute("title", tooltip);
    elements.macroLeverage.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.socialFinancing.infoButton) {
    const tooltip = dict.socialFinancingTooltip || "";
    elements.socialFinancing.infoButton.setAttribute("title", tooltip);
    elements.socialFinancing.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.cpiMonthly.infoButton) {
    const tooltip = dict.cpiTooltip || "";
    elements.cpiMonthly.infoButton.setAttribute("title", tooltip);
    elements.cpiMonthly.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.ppiMonthly.infoButton) {
    const tooltip = dict.ppiTooltip || "";
    elements.ppiMonthly.infoButton.setAttribute("title", tooltip);
    elements.ppiMonthly.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.lprRate.infoButton) {
    const tooltip = dict.lprRateTooltip || "";
    elements.lprRate.infoButton.setAttribute("title", tooltip);
    elements.lprRate.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.shiborRate.infoButton) {
    const tooltip = dict.shiborRateTooltip || "";
    elements.shiborRate.infoButton.setAttribute("title", tooltip);
    elements.shiborRate.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.pmiMonthly.infoButton) {
    const tooltip = dict.pmiTooltip || "";
    elements.pmiMonthly.infoButton.setAttribute("title", tooltip);
    elements.pmiMonthly.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
  if (elements.m2Monthly.infoButton) {
    const tooltip = dict.m2Tooltip || "";
    elements.m2Monthly.infoButton.setAttribute("title", tooltip);
    elements.m2Monthly.infoButton.setAttribute("aria-label", tooltip || "Info");
  }
}

function jobStatusLabel(status) {
  const dict = translations[currentLang];
  switch (status) {
    case "running":
      return dict.statusRunning;
    case "success":
      return dict.statusSuccess;
    case "failed":
      return dict.statusFailed;
    case "idle":
      return dict.statusIdle;
    default:
      return dict.statusUnknown;
  }
}
function updateJobCard(cardElements, snapshot) {
  if (!cardElements || !cardElements.status) {
    return;
  }
  cardElements.status.textContent = jobStatusLabel(snapshot.status);
  if (cardElements.updated) {
    cardElements.updated.textContent = formatDateTime(
      snapshot.finishedAt || snapshot.startedAt
    );
  }
  if (cardElements.duration) {
    cardElements.duration.textContent = formatDuration(snapshot.lastDuration);
  }
  if (cardElements.tradeDate) {
    cardElements.tradeDate.textContent = formatTradeDate(snapshot.lastMarket);
  }
  if (cardElements.rows) {
    cardElements.rows.textContent = formatNumber(snapshot.totalRows);
  }
  if (cardElements.message) {
    cardElements.message.textContent =
      snapshot.message || translations[currentLang].messageNone;
  }
  const isRunning = snapshot.status === "running";
  if (cardElements.progress) {
    updateProgressBar(
      cardElements.progress,
      isRunning ? snapshot.progress : 0
    );
  }
  if (cardElements.button) {
    cardElements.button.disabled = isRunning;
  }
}

function updateProgressBar(bar, progress) {
  if (!bar) {
    return;
  }
  const clamped = Math.max(0, Math.min(1, progress ?? 0));
  bar.style.width = `${(clamped * 100).toFixed(0)}%`;
  if (clamped > 0) {
    bar.classList.add("is-active");
  } else {
    bar.classList.remove("is-active");
  }
}

async function loadStatus() {
  try {
    const response = await fetch(`${API_BASE}/control/status`);
    const data = await response.json();
    const jobs = data.jobs || {};
    const stockSnapshot = jobs.stock_basic || {
      status: "idle",
      progress: 0,
    };
    const conceptDirectorySnapshot = jobs.concept_directory || {
      status: "idle",
      progress: 0,
    };
    const dailySnapshot = jobs.daily_trade || {
      status: "idle",
      progress: 0,
    };
    const metricsSnapshot = jobs.daily_trade_metrics || {
      status: "idle",
      progress: 0,
    };
    const fundamentalSnapshot = jobs.fundamental_metrics || {
      status: "idle",
      progress: 0,
    };
    const macroAggregateSnapshot = jobs.macro_aggregate || {
      status: "idle",
      progress: 0,
    };
    const fundFlowAggregateSnapshot = jobs.fund_flow_aggregate || {
      status: "idle",
      progress: 0,
    };
    const peripheralAggregateSnapshot = jobs.peripheral_aggregate || {
      status: "idle",
      progress: 0,
    };
    const indicatorSnapshot = jobs.daily_indicator || {
      status: "idle",
      progress: 0,
    };
    const indexHistorySnapshot = jobs.index_history || {
      status: "idle",
      progress: 0,
    };
    const incomeSnapshot = jobs.income_statement || {
      status: "idle",
      progress: 0,
    };
    const cashflowSnapshot = jobs.cashflow_statements || {
      status: "idle",
      progress: 0,
    };
    const balanceSheetSnapshot = jobs.balance_sheet_statements || {
      status: "idle",
      progress: 0,
    };
    const financialSnapshot = jobs.financial_indicator || {
      status: "idle",
      progress: 0,
    };
    const expressSnapshot = jobs.performance_express || {
      status: "idle",
      progress: 0,
    };
    const forecastSnapshot = jobs.performance_forecast || {
      status: "idle",
      progress: 0,
    };
    const profitForecastSnapshot = jobs.profit_forecast || {
      status: "idle",
      progress: 0,
    };
    const globalIndexSnapshot = jobs.global_index || {
      status: "idle",
      progress: 0,
    };
    const realtimeSnapshot = jobs.realtime_index || {
      status: "idle",
      progress: 0,
    };
    const realtimeTradeSnapshot = jobs.realtime_trade || {
      status: "idle",
      progress: 0,
    };
    const leverageSnapshot = jobs.leverage_ratio || {
      status: "idle",
      progress: 0,
    };
    const socialFinancingSnapshot = jobs.social_financing || {
      status: "idle",
      progress: 0,
    };
    const cpiSnapshot = jobs.cpi_monthly || {
      status: "idle",
      progress: 0,
    };
    const ppiSnapshot = jobs.ppi_monthly || {
      status: "idle",
      progress: 0,
    };
    const lprRateSnapshot = jobs.lpr_rate || {
      status: "idle",
      progress: 0,
    };
    const shiborRateSnapshot = jobs.shibor_rate || {
      status: "idle",
      progress: 0,
    };
    const pmiSnapshot = jobs.pmi_monthly || {
      status: "idle",
      progress: 0,
    };
    const m2Snapshot = jobs.m2_monthly || {
      status: "idle",
      progress: 0,
    };
    const macroInsightSnapshot = jobs.macro_insight || {
      status: "idle",
      progress: 0,
    };
    const marketInsightSnapshot = jobs.market_insight || {
      status: "idle",
      progress: 0,
    };
    const dollarIndexSnapshot = jobs.dollar_index || {
      status: "idle",
      progress: 0,
    };
    const rmbMidpointSnapshot = jobs.rmb_midpoint || {
      status: "idle",
      progress: 0,
    };
    const futuresRealtimeSnapshot = jobs.futures_realtime || {
      status: "idle",
      progress: 0,
    };
    const peripheralInsightSnapshot = jobs.peripheral_insight || {
      status: "idle",
      progress: 0,
    };
    const fedStatementsSnapshot = jobs.fed_statements || {
      status: "idle",
      progress: 0,
    };
    const industryFundFlowSnapshot = jobs.industry_fund_flow || {
      status: "idle",
      progress: 0,
    };
    const conceptFundFlowSnapshot = jobs.concept_fund_flow || {
      status: "idle",
      progress: 0,
    };
    const conceptIndexHistorySnapshot = jobs.concept_index_history || {
      status: "idle",
      progress: 0,
    };
    const conceptInsightSnapshot = jobs.concept_insight || {
      status: "idle",
      progress: 0,
    };
    const industryInsightSnapshot = jobs.industry_insight || {
      status: "idle",
      progress: 0,
    };
    const individualFundFlowSnapshot = jobs.individual_fund_flow || {
      status: "idle",
      progress: 0,
    };
    const bigDealFundFlowSnapshot = jobs.big_deal_fund_flow || {
      status: "idle",
      progress: 0,
    };
    const marginAccountSnapshot = jobs.margin_account || {
      status: "idle",
      progress: 0,
    };
    const marketFundFlowSnapshot = jobs.market_fund_flow || {
      status: "idle",
      progress: 0,
    };
    const marketActivitySnapshot = jobs.market_activity || {
      status: "idle",
      progress: 0,
    };
    const mainBusinessSnapshot = jobs.stock_main_business || {
      status: "idle",
      progress: 0,
    };
    const mainCompositionSnapshot = jobs.stock_main_composition || {
      status: "idle",
      progress: 0,
    };
    const globalFlashSnapshot = jobs.global_flash || {
      status: "idle",
      progress: 0,
    };
    const globalFlashClassifySnapshot = jobs.global_flash_classification || {
      status: "idle",
      progress: 0,
    };
    const breakfastSnapshot = jobs.finance_breakfast || {
      status: "idle",
      progress: 0,
    };

    updateJobCard(elements.stockBasic, stockSnapshot);
    updateJobCard(elements.dailyTrade, dailySnapshot);
    updateJobCard(elements.dailyTradeMetrics, metricsSnapshot);
    updateJobCard(elements.dailyIndicator, indicatorSnapshot);
    updateJobCard(elements.realtimeTrade, realtimeTradeSnapshot);
    updateJobCard(elements.indexHistory, indexHistorySnapshot);
    updateJobCard(elements.fundamentalMetrics, fundamentalSnapshot);
    updateJobCard(elements.macroAggregate, macroAggregateSnapshot);
    updateJobCard(elements.fundFlowAggregate, fundFlowAggregateSnapshot);
    updateJobCard(elements.peripheralAggregate, peripheralAggregateSnapshot);
    updateJobCard(elements.incomeStatement, incomeSnapshot);
    updateJobCard(elements.cashflowStatement, cashflowSnapshot);
    updateJobCard(elements.balanceSheet, balanceSheetSnapshot);
    updateJobCard(elements.financialIndicator, financialSnapshot);
    updateJobCard(elements.performanceExpress, expressSnapshot);
    updateJobCard(elements.performanceForecast, forecastSnapshot);
    updateJobCard(elements.globalIndex, globalIndexSnapshot);
    updateJobCard(elements.realtimeIndex, realtimeSnapshot);
    updateJobCard(elements.macroLeverage, leverageSnapshot);
    updateJobCard(elements.macroInsight, macroInsightSnapshot);
    updateJobCard(elements.socialFinancing, socialFinancingSnapshot);
    updateJobCard(elements.cpiMonthly, cpiSnapshot);
    updateJobCard(elements.ppiMonthly, ppiSnapshot);
    updateJobCard(elements.lprRate, lprRateSnapshot);
    updateJobCard(elements.shiborRate, shiborRateSnapshot);
    updateJobCard(elements.pmiMonthly, pmiSnapshot);
    updateJobCard(elements.m2Monthly, m2Snapshot);
    updateJobCard(elements.rmbMidpoint, rmbMidpointSnapshot);
    updateJobCard(elements.futuresRealtime, futuresRealtimeSnapshot);
    updateJobCard(elements.peripheralInsight, peripheralInsightSnapshot);
    updateJobCard(elements.fedStatements, fedStatementsSnapshot);
    updateJobCard(elements.dollarIndex, dollarIndexSnapshot);
    updateJobCard(elements.marketInsight, marketInsightSnapshot);
    updateJobCard(elements.profitForecast, profitForecastSnapshot);
    updateJobCard(elements.industryFundFlow, industryFundFlowSnapshot);
    updateJobCard(elements.conceptFundFlow, conceptFundFlowSnapshot);
    updateJobCard(elements.conceptIndexHistory, conceptIndexHistorySnapshot);
    updateJobCard(elements.conceptInsight, conceptInsightSnapshot);
    updateJobCard(elements.industryInsight, industryInsightSnapshot);
    updateJobCard(elements.individualFundFlow, individualFundFlowSnapshot);
    updateJobCard(elements.marginAccount, marginAccountSnapshot);
    updateJobCard(elements.marketFundFlow, marketFundFlowSnapshot);
    updateJobCard(elements.marketActivity, marketActivitySnapshot);
    updateJobCard(elements.bigDealFundFlow, bigDealFundFlowSnapshot);
    updateJobCard(elements.stockMainBusiness, mainBusinessSnapshot);
    updateJobCard(elements.stockMainComposition, mainCompositionSnapshot);
    updateJobCard(elements.conceptDirectory, conceptDirectorySnapshot);
    updateJobCard(elements.globalFlash, globalFlashSnapshot);
    updateJobCard(elements.globalFlashClassification, globalFlashClassifySnapshot);
    updateJobCard(elements.financeBreakfast, breakfastSnapshot);

    if (data.config) {
      configState.includeST = !!data.config.includeST;
      configState.includeDelisted = !!data.config.includeDelisted;
      const windowDays = Number(data.config.dailyTradeWindowDays);
      if (Number.isFinite(windowDays) && windowDays > 0) {
        configState.dailyTradeWindowDays = windowDays;
      }
    }


    const shouldPoll = [
      stockSnapshot,
      dailySnapshot,
      metricsSnapshot,
      indicatorSnapshot,
      realtimeTradeSnapshot,
      indexHistorySnapshot,
      fundamentalSnapshot,
      macroAggregateSnapshot,
      fundFlowAggregateSnapshot,
      peripheralAggregateSnapshot,
      incomeSnapshot,
      financialSnapshot,
      expressSnapshot,
      forecastSnapshot,
      globalIndexSnapshot,
      realtimeSnapshot,
      leverageSnapshot,
      macroInsightSnapshot,
      marketInsightSnapshot,
      socialFinancingSnapshot,
      cpiSnapshot,
      ppiSnapshot,
      lprRateSnapshot,
      shiborRateSnapshot,
      pmiSnapshot,
      m2Snapshot,
      rmbMidpointSnapshot,
      futuresRealtimeSnapshot,
      peripheralInsightSnapshot,
      fedStatementsSnapshot,
      dollarIndexSnapshot,
      industryFundFlowSnapshot,
      conceptFundFlowSnapshot,
      conceptIndexHistorySnapshot,
      conceptInsightSnapshot,
      industryInsightSnapshot,
      individualFundFlowSnapshot,
      marginAccountSnapshot,
      marketFundFlowSnapshot,
      marketActivitySnapshot,
      bigDealFundFlowSnapshot,
      mainBusinessSnapshot,
      mainCompositionSnapshot,
      globalFlashSnapshot,
      globalFlashClassifySnapshot,
      breakfastSnapshot,
      conceptDirectorySnapshot,
    ].some((snapshot) => snapshot.status === "running");
    if (shouldPoll && !pollTimer) {
      pollTimer = setInterval(loadStatus, 3000);
    } else if (!shouldPoll && pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  } catch (error) {
    console.error("Failed to load status", error);
  }
}

async function triggerJob(endpoint, payload) {
  const body = payload ? JSON.stringify(payload) : "{}";
  try {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
    if (!response.ok) {
      let errorMessage = `Request failed with status ${response.status}`;
      try {
        const data = await response.json();
        if (data && typeof data.detail === "string") {
          errorMessage = data.detail;
        } else if (data && typeof data.message === "string") {
          errorMessage = data.message;
        }
      } catch (parseError) {
        /* no-op */
      }
      throw new Error(errorMessage);
    }
    setTimeout(loadStatus, 500);
    if (!pollTimer) {
      pollTimer = setInterval(loadStatus, 3000);
    }
    return { ok: true };
  } catch (error) {
    console.error("Failed to start job", error);
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function setJobPending(cardElements) {
  if (!cardElements || !cardElements.status) {
    return;
  }
  const dict = translations[currentLang];
  const statusText = dict.statusStarting || dict.statusRunning || "";
  cardElements.status.textContent = statusText;
  if (cardElements.message) {
    cardElements.message.textContent = dict.jobStartingMessage || dict.statusStarting || "";
  }
  if (cardElements.rows) {
    cardElements.rows.textContent = "--";
  }
  if (cardElements.button) {
    cardElements.button.disabled = true;
  }
  if (cardElements.progress) {
    updateProgressBar(cardElements.progress, 0.1);
  }
}

function setJobError(cardElements, errorMessage) {
  if (!cardElements || !cardElements.status) {
    return;
  }
  const dict = translations[currentLang];
  cardElements.status.textContent = dict.statusFailed || "Failed";
  if (cardElements.message) {
    cardElements.message.textContent = errorMessage || dict.jobStartFailed || dict.statusFailed || "";
  }
  if (cardElements.button) {
    cardElements.button.disabled = false;
  }
  if (cardElements.progress) {
    updateProgressBar(cardElements.progress, 0);
  }
}

function triggerJobForCard(cardElements, endpoint, payload) {
  setJobPending(cardElements);
  triggerJob(endpoint, payload).then((result) => {
    if (!result?.ok) {
      setJobError(cardElements, result?.error);
    }
  });
}



window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

function initLanguageSwitch() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLang(btn.dataset.lang))
  );
}

function initControlTabs() {
  const tabLinks = Array.from(document.querySelectorAll(".control-tabs__link"));
  if (!tabLinks.length) {
    return;
  }

  const contentRoot = document.querySelector(".content");
  const groups = tabLinks
    .map((link) => {
      const href = link.getAttribute("href");
      if (!href || !href.startsWith("#")) {
        return null;
      }
      const target = document.getElementById(href.slice(1));
      return target ? { link, target } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.target.offsetTop - b.target.offsetTop);

  if (!groups.length) {
    return;
  }

  const setActive = (activeLink) => {
    tabLinks.forEach((link) =>
      link.classList.toggle("active", link === activeLink)
    );
  };

  setActive(groups[0].link);

  tabLinks.forEach((link) => {
    link.addEventListener("click", (event) => {
      const href = link.getAttribute("href");
      if (!href || !href.startsWith("#")) {
        return;
      }
      event.preventDefault();
      const target = document.getElementById(href.slice(1));
      if (!target) {
        return;
      }
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      setActive(link);
    });
  });

  if (!("IntersectionObserver" in window)) {
    return;
  }

  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort(
          (a, b) => a.boundingClientRect.top - b.boundingClientRect.top
        );
      if (visible.length) {
        const topTarget = visible[0].target;
        const match = groups.find((item) => item.target === topTarget);
        if (match) {
          setActive(match.link);
        }
        return;
      }
      const scrollTop =
        (contentRoot ? contentRoot.scrollTop : window.scrollY) + 1;
      let current = groups[0];
      for (const item of groups) {
        if (item.target.offsetTop <= scrollTop) {
          current = item;
        }
      }
      setActive(current.link);
    },
    {
      root: contentRoot || null,
      threshold: 0.3,
      rootMargin: "-20% 0px -55% 0px",
    }
  );

  groups.forEach((item) => observer.observe(item.target));
}

function initActions() {
  elements.stockBasic.button.addEventListener("click", () =>
    triggerJob("/control/sync/stock-basic", {})
  );
  elements.dailyTrade.button.addEventListener("click", () => {
    triggerJobForCard(elements.dailyTrade, "/control/sync/daily-trade", {
      window_days: configState.dailyTradeWindowDays || undefined,
    });
  });
  elements.dailyTradeMetrics.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-trade-metrics", {})
  );
  elements.dailyIndicator.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-indicators", {})
  );
  if (elements.realtimeTrade.button) {
    elements.realtimeTrade.button.addEventListener("click", () =>
      triggerJob("/control/sync/realtime-trade", { syncAll: true })
    );
  }
  if (elements.indexHistory.button) {
    elements.indexHistory.button.addEventListener("click", () =>
      triggerJob("/control/sync/index-history", {})
    );
  }
  if (elements.realtimeIndex.button) {
    elements.realtimeIndex.button.addEventListener("click", () =>
      triggerJob("/control/sync/realtime-indices", {})
    );
  }
  elements.fundamentalMetrics.button.addEventListener("click", () =>
    triggerJob("/control/sync/fundamental-metrics", {})
  );
  elements.incomeStatement.button.addEventListener("click", () =>
    triggerJob("/control/sync/income-statements", {})
  );
  if (elements.cashflowStatement?.button) {
    elements.cashflowStatement.button.addEventListener("click", () =>
      triggerJob("/control/sync/cashflow-statements", {})
    );
  }
  if (elements.balanceSheet?.button) {
    elements.balanceSheet.button.addEventListener("click", () =>
      triggerJob("/control/sync/balance-sheet-statements", {})
    );
  }
  elements.financialIndicator.button.addEventListener("click", () =>
    triggerJob("/control/sync/financial-indicators", {})
  );
  if (elements.conceptDirectory.button) {
    elements.conceptDirectory.button.addEventListener("click", () =>
      triggerJob("/control/sync/concept-directory", {})
    );
  }
  elements.performanceExpress.button.addEventListener("click", () =>
    triggerJob("/control/sync/performance-express", {})
  );
  elements.performanceForecast.button.addEventListener("click", () =>
    triggerJob("/control/sync/performance-forecast", {})
  );
  if (elements.profitForecast.button) {
    elements.profitForecast.button.addEventListener("click", () =>
      triggerJobForCard(elements.profitForecast, "/control/sync/profit-forecast", {})
    );
  }
  if (elements.industryFundFlow.button) {
    elements.industryFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/industry-fund-flow", {
        symbols: INDUSTRY_FUND_FLOW_SYMBOLS,
      })
    );
  }
  if (elements.conceptFundFlow.button) {
    elements.conceptFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/concept-fund-flow", {
        symbols: INDUSTRY_FUND_FLOW_SYMBOLS,
      })
    );
  }
  if (elements.conceptIndexHistory.button) {
    elements.conceptIndexHistory.button.addEventListener("click", () => {
      const conceptInput = elements.conceptIndexHistory.conceptInput;
      const rawConcepts = conceptInput ? conceptInput.value : "";
      const concepts = normalizeConceptList(rawConcepts);
      if (!concepts.length) {
        const dict = translations[currentLang] || translations.zh || translations.en;
        window.alert(
          (dict && dict.conceptIndexHistoryConceptRequired) ||
            "Please enter at least one concept before running."
        );
        return;
      }
      const lookbackField = elements.conceptIndexHistory.lookbackInput;
      let lookbackDays = Number(lookbackField ? lookbackField.value : 0);
      if (!Number.isFinite(lookbackDays)) {
        lookbackDays = 90;
      }
      lookbackDays = Math.max(30, Math.min(lookbackDays, 365));

      const endDateField = elements.conceptIndexHistory.endDateInput;
      const endDate =
        parseDateInputValue(endDateField && endDateField.value) || new Date();
      const endDateStr = formatYmd(endDate);
      const startDate = new Date(endDate);
      startDate.setDate(endDate.getDate() - (lookbackDays - 1));
      const startDateStr = formatYmd(startDate);
      if (!startDateStr || !endDateStr) {
        console.warn("Invalid date range for concept index history sync.");
        return;
      }
      triggerJob("/control/sync/concept-index-history", {
        concepts,
        startDate: startDateStr,
        endDate: endDateStr,
      });
    });
  }
  if (elements.conceptInsight.button) {
    elements.conceptInsight.button.addEventListener("click", () =>
      triggerJob("/control/sync/concept-insight", {
        lookbackHours: 48,
        conceptLimit: 10,
        runLLM: true,
        refreshIndexHistory: true,
      })
    );
  }
  if (elements.industryInsight.button) {
    elements.industryInsight.button.addEventListener("click", () =>
      triggerJob("/control/sync/industry-insight", {
        lookbackHours: 48,
        industryLimit: 5,
        runLLM: true,
      })
    );
  }
  if (elements.individualFundFlow.button) {
    elements.individualFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/individual-fund-flow", {
        symbols: INDUSTRY_FUND_FLOW_SYMBOLS,
      })
    );
  }
  if (elements.marginAccount.button) {
    elements.marginAccount.button.addEventListener("click", () =>
      triggerJob("/control/sync/margin-account", {})
    );
  }
  if (elements.marketFundFlow.button) {
    elements.marketFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/market-fund-flow", {})
    );
  }
  if (elements.marketActivity.button) {
    elements.marketActivity.button.addEventListener("click", () =>
      triggerJob("/control/sync/market-activity", {})
    );
  }
  if (elements.bigDealFundFlow.button) {
    elements.bigDealFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/big-deal-fund-flow", {})
    );
  }
  if (elements.fundFlowAggregate.button) {
    elements.fundFlowAggregate.button.addEventListener("click", () =>
      triggerJob("/control/sync/fund-flow-aggregate", {})
    );
  }
  if (elements.macroAggregate.button) {
    elements.macroAggregate.button.addEventListener("click", () =>
      triggerJob("/control/sync/macro-aggregate", {})
    );
  }
  if (elements.peripheralAggregate.button) {
    elements.peripheralAggregate.button.addEventListener("click", () =>
      triggerJob("/control/sync/peripheral-aggregate", {})
    );
  }
  if (elements.globalIndex.button) {
    elements.globalIndex.button.addEventListener("click", () =>
      triggerJob("/control/sync/global-indices", {})
    );
  }
  if (elements.macroLeverage.button) {
    elements.macroLeverage.button.addEventListener("click", () =>
      triggerJob("/control/sync/leverage-ratio", {})
    );
  }
  if (elements.macroInsight.button) {
    elements.macroInsight.button.addEventListener("click", () =>
      triggerJob("/control/sync/macro-insight", { runLLM: true })
    );
  }
  if (elements.marketInsight.button) {
    elements.marketInsight.button.addEventListener("click", () =>
      triggerJob("/control/sync/market-insight", { lookbackHours: 24, articleLimit: 40 })
    );
  }
  if (elements.socialFinancing.button) {
    elements.socialFinancing.button.addEventListener("click", () =>
      triggerJob("/control/sync/social-financing", {})
    );
  }
  if (elements.cpiMonthly.button) {
    elements.cpiMonthly.button.addEventListener("click", () =>
      triggerJob("/control/sync/cpi", {})
    );
  }
  if (elements.ppiMonthly.button) {
    elements.ppiMonthly.button.addEventListener("click", () =>
      triggerJob("/control/sync/ppi", {})
    );
  }
  if (elements.lprRate.button) {
    elements.lprRate.button.addEventListener("click", () =>
      triggerJob("/control/sync/lpr", {})
    );
  }
  if (elements.shiborRate.button) {
    elements.shiborRate.button.addEventListener("click", () =>
      triggerJob("/control/sync/shibor", {})
    );
  }
  if (elements.pmiMonthly.button) {
    elements.pmiMonthly.button.addEventListener("click", () =>
      triggerJob("/control/sync/pmi", {})
    );
  }
  if (elements.m2Monthly.button) {
    elements.m2Monthly.button.addEventListener("click", () =>
      triggerJob("/control/sync/m2", {})
    );
  }
  if (elements.macroLeverage.infoButton) {
    elements.macroLeverage.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.macroLeverageTooltip) ||
        (translations.en && translations.en.macroLeverageTooltip) ||
        "Quarterly release: macro leverage reports are published about 1-2 months after each quarter ends.";
      window.alert(message);
    });
  }
  if (elements.cpiMonthly.infoButton) {
    elements.cpiMonthly.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.cpiTooltip) ||
        (translations.en && translations.en.cpiTooltip) ||
        "Monthly release: NBS publishes CPI around the 9th-10th of the following month.";
      window.alert(message);
    });
  }
  if (elements.ppiMonthly.infoButton) {
    elements.ppiMonthly.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.ppiTooltip) ||
        (translations.en && translations.en.ppiTooltip) ||
        "Monthly release: NBS publishes PPI around the 9th-10th of the following month.";
      window.alert(message);
    });
  }
  if (elements.lprRate.infoButton) {
    elements.lprRate.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.lprRateTooltip) ||
        (translations.en && translations.en.lprRateTooltip) ||
        "Irregular release: trigger after the PBOC publishes a new loan prime rate.";
      window.alert(message);
    });
  }
  if (elements.shiborRate.infoButton) {
    elements.shiborRate.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.shiborRateTooltip) ||
        (translations.en && translations.en.shiborRateTooltip) ||
        "Daily SHIBOR quotes typically refresh around 11:30 Beijing time on trading days.";
      window.alert(message);
    });
  }
  if (elements.pmiMonthly.infoButton) {
    elements.pmiMonthly.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.pmiTooltip) ||
        (translations.en && translations.en.pmiTooltip) ||
        "Monthly release: NBS publishes PMI around the 9th-10th of the following month.";
      window.alert(message);
    });
  }
  if (elements.m2Monthly.infoButton) {
    elements.m2Monthly.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.m2Tooltip) ||
        (translations.en && translations.en.m2Tooltip) ||
        "Monthly release: M2 data typically arrives between the 10th-12th around 16:00.";
      window.alert(message);
    });
  }
  if (elements.socialFinancing.infoButton) {
    elements.socialFinancing.infoButton.addEventListener("click", () => {
      const dict = translations[currentLang] || translations.en;
      const message =
        (dict && dict.socialFinancingTooltip) ||
        (translations.en && translations.en.socialFinancingTooltip) ||
        "Monthly release: social financing data usually arrives around the 10th-12th each month.";
      window.alert(message);
    });
  }
  if (elements.rmbMidpoint.button) {
    elements.rmbMidpoint.button.addEventListener("click", () =>
      triggerJob("/control/sync/rmb-midpoint", {})
    );
  }
  if (elements.futuresRealtime.button) {
    elements.futuresRealtime.button.addEventListener("click", () =>
      triggerJob("/control/sync/futures-realtime", {})
    );
  }
  if (elements.peripheralInsight.button) {
    elements.peripheralInsight.button.addEventListener("click", () =>
      triggerJob("/control/sync/peripheral-summary", {
        runLLM: true,
      })
    );
  }
  if (elements.fedStatements.button) {
    elements.fedStatements.button.addEventListener("click", () =>
      triggerJob("/control/sync/fed-statements", {
        limit: 5,
      })
    );
  }
  if (elements.dollarIndex.button) {
    elements.dollarIndex.button.addEventListener("click", () =>
      triggerJob("/control/sync/dollar-index", {})
    );
  }
  if (elements.stockMainBusiness.button) {
    elements.stockMainBusiness.button.addEventListener("click", () =>
      triggerJob("/control/sync/stock-main-business", {})
    );
  }
  if (elements.stockMainComposition.button) {
    elements.stockMainComposition.button.addEventListener("click", () =>
      triggerJob("/control/sync/stock-main-composition", {})
    );
  }
  if (elements.globalFlash.button) {
    elements.globalFlash.button.addEventListener("click", () =>
      triggerJob("/control/sync/global-flash", {})
    );
  }
  if (elements.globalFlashClassification.button) {
    elements.globalFlashClassification.button.addEventListener("click", () => {
      const attr = elements.globalFlashClassification.button.getAttribute("data-batch-size");
      const batchSize = Number(attr);
      triggerJob("/control/sync/global-flash-classification", {
        batchSize: Number.isFinite(batchSize) && batchSize > 0 ? batchSize : 10,
      });
    });
  }
  elements.financeBreakfast.button.addEventListener("click", () =>
    triggerJob("/control/sync/finance-breakfast", {})
  );
}

async function prefillConceptIndexHistoryConcepts() {
  const conceptField = elements.conceptIndexHistory?.conceptInput;
  if (!conceptField || conceptField.value.trim()) {
    return;
  }
  try {
    const response = await fetch(
      `${API_BASE}/fund-flow/concept?symbol=${encodeURIComponent("即时")}&limit=12`
    );
    if (!response.ok) {
      return;
    }
    const data = await response.json();
    const names = Array.isArray(data.items)
      ? data.items
          .map((item) => item.concept)
          .filter((name) => typeof name === "string" && name.trim())
      : [];
    if (names.length && !conceptField.value.trim()) {
      const deduped = Array.from(new Set(names));
      conceptField.value = deduped.slice(0, 10).join("\n");
    }
  } catch (error) {
    console.warn("Failed to prefill concept list", error);
  }
}

// Boot
initLanguageSwitch();
initControlTabs();
initActions();
setLang(currentLang);
loadStatus();
prefillConceptIndexHistoryConcepts();
