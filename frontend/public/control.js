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
  dailyIndicator: {
    status: document.getElementById("daily-indicator-status"),
    updated: document.getElementById("daily-indicator-updated"),
    duration: document.getElementById("daily-indicator-duration"),
    rows: document.getElementById("daily-indicator-rows"),
    message: document.getElementById("daily-indicator-message"),
    progress: document.getElementById("daily-indicator-progress"),
    button: document.getElementById("run-daily-indicator"),
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
  peripheralInsight: {
    status: document.getElementById("peripheral-insight-status"),
    updated: document.getElementById("peripheral-insight-updated"),
    duration: document.getElementById("peripheral-insight-duration"),
    rows: document.getElementById("peripheral-insight-rows"),
    message: document.getElementById("peripheral-insight-message"),
    progress: document.getElementById("peripheral-insight-progress"),
    button: document.getElementById("run-peripheral-insight"),
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
  conceptFundFlow: {
    status: document.getElementById("concept-fund-flow-status"),
    updated: document.getElementById("concept-fund-flow-updated"),
    duration: document.getElementById("concept-fund-flow-duration"),
    rows: document.getElementById("concept-fund-flow-rows"),
    message: document.getElementById("concept-fund-flow-message"),
    progress: document.getElementById("concept-fund-flow-progress"),
    button: document.getElementById("run-concept-fund-flow"),
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
  cardElements.status.textContent = jobStatusLabel(snapshot.status);
  cardElements.updated.textContent = formatDateTime(
    snapshot.finishedAt || snapshot.startedAt
  );
  if (cardElements.duration) {
    cardElements.duration.textContent = formatDuration(snapshot.lastDuration);
  }
  if (cardElements.tradeDate) {
    cardElements.tradeDate.textContent = formatTradeDate(snapshot.lastMarket);
  }
  cardElements.rows.textContent = formatNumber(snapshot.totalRows);
  cardElements.message.textContent =
    snapshot.message || translations[currentLang].messageNone;
  const isRunning = snapshot.status === "running";
  updateProgressBar(
    cardElements.progress,
    isRunning ? snapshot.progress : 0
  );
  cardElements.button.disabled = isRunning;
}

function updateProgressBar(bar, progress) {
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
    const indicatorSnapshot = jobs.daily_indicator || {
      status: "idle",
      progress: 0,
    };
    const incomeSnapshot = jobs.income_statement || {
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
  const individualFundFlowSnapshot = jobs.individual_fund_flow || {
    status: "idle",
    progress: 0,
  };
  const bigDealFundFlowSnapshot = jobs.big_deal_fund_flow || {
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
  const breakfastSnapshot = jobs.finance_breakfast || {
    status: "idle",
    progress: 0,
  };

    updateJobCard(elements.stockBasic, stockSnapshot);
    updateJobCard(elements.dailyTrade, dailySnapshot);
    updateJobCard(elements.dailyTradeMetrics, metricsSnapshot);
    updateJobCard(elements.dailyIndicator, indicatorSnapshot);
    updateJobCard(elements.fundamentalMetrics, fundamentalSnapshot);
    updateJobCard(elements.incomeStatement, incomeSnapshot);
    updateJobCard(elements.financialIndicator, financialSnapshot);
    updateJobCard(elements.performanceExpress, expressSnapshot);
    updateJobCard(elements.performanceForecast, forecastSnapshot);
    updateJobCard(elements.globalIndex, globalIndexSnapshot);
    updateJobCard(elements.rmbMidpoint, rmbMidpointSnapshot);
    updateJobCard(elements.futuresRealtime, futuresRealtimeSnapshot);
    updateJobCard(elements.peripheralInsight, peripheralInsightSnapshot);
    updateJobCard(elements.fedStatements, fedStatementsSnapshot);
    updateJobCard(elements.dollarIndex, dollarIndexSnapshot);
    updateJobCard(elements.profitForecast, profitForecastSnapshot);
    updateJobCard(elements.industryFundFlow, industryFundFlowSnapshot);
    updateJobCard(elements.conceptFundFlow, conceptFundFlowSnapshot);
    updateJobCard(elements.individualFundFlow, individualFundFlowSnapshot);
    updateJobCard(elements.bigDealFundFlow, bigDealFundFlowSnapshot);
    updateJobCard(elements.stockMainBusiness, mainBusinessSnapshot);
    updateJobCard(elements.stockMainComposition, mainCompositionSnapshot);
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
      fundamentalSnapshot,
      incomeSnapshot,
      financialSnapshot,
      expressSnapshot,
      forecastSnapshot,
      globalIndexSnapshot,
      rmbMidpointSnapshot,
      futuresRealtimeSnapshot,
      peripheralInsightSnapshot,
      fedStatementsSnapshot,
      dollarIndexSnapshot,
      industryFundFlowSnapshot,
      conceptFundFlowSnapshot,
      individualFundFlowSnapshot,
      bigDealFundFlowSnapshot,
      mainBusinessSnapshot,
      mainCompositionSnapshot,
      breakfastSnapshot,
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
  try {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    setTimeout(loadStatus, 500);
    if (!pollTimer) {
      pollTimer = setInterval(loadStatus, 3000);
    }
  } catch (error) {
    console.error("Failed to start job", error);
  }
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
  elements.dailyTrade.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-trade", {
      window_days: configState.dailyTradeWindowDays || undefined,
    })
  );
  elements.dailyTradeMetrics.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-trade-metrics", {})
  );
  elements.dailyIndicator.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-indicators", {})
  );
  elements.fundamentalMetrics.button.addEventListener("click", () =>
    triggerJob("/control/sync/fundamental-metrics", {})
  );
  elements.incomeStatement.button.addEventListener("click", () =>
    triggerJob("/control/sync/income-statements", {})
  );
  elements.financialIndicator.button.addEventListener("click", () =>
    triggerJob("/control/sync/financial-indicators", {})
  );
  elements.performanceExpress.button.addEventListener("click", () =>
    triggerJob("/control/sync/performance-express", {})
  );
  elements.performanceForecast.button.addEventListener("click", () =>
    triggerJob("/control/sync/performance-forecast", {})
  );
  if (elements.profitForecast.button) {
    elements.profitForecast.button.addEventListener("click", () =>
      triggerJob("/control/sync/profit-forecast", {})
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
  if (elements.individualFundFlow.button) {
    elements.individualFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/individual-fund-flow", {
        symbols: INDUSTRY_FUND_FLOW_SYMBOLS,
      })
    );
  }
  if (elements.bigDealFundFlow.button) {
    elements.bigDealFundFlow.button.addEventListener("click", () =>
      triggerJob("/control/sync/big-deal-fund-flow", {})
    );
  }
  if (elements.globalIndex.button) {
    elements.globalIndex.button.addEventListener("click", () =>
      triggerJob("/control/sync/global-indices", {})
    );
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
  elements.financeBreakfast.button.addEventListener("click", () =>
    triggerJob("/control/sync/finance-breakfast", {})
  );
}

// Boot
initLanguageSwitch();
initControlTabs();
initActions();
setLang(currentLang);
loadStatus();
