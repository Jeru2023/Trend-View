const translations = getTranslations("controlPanel");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

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
let pollTimer = null;

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
  config: {
    includeSt: document.getElementById("config-include-st"),
    includeDelisted: document.getElementById("config-include-delisted"),
    window: document.getElementById("config-window"),
    save: document.getElementById("save-config"),
  },
};

function formatDateTime(value) {
  if (!value) return "—";
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  try {
    return new Date(value).toLocaleString(locale);
  } catch (err) {
    return value;
  }
}

function formatTradeDate(value) {
  if (!value) return "—";
  const str = String(value).trim();
  if (/^\d{8}$/.test(str)) {
    return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
  }
  return str;
}

function formatNumber(value) {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale).format(num);
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "—";
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
    const breakfastSnapshot = jobs.finance_breakfast || {
      status: "idle",
      progress: 0,
    };

    updateJobCard(elements.stockBasic, stockSnapshot);
    updateJobCard(elements.dailyTrade, dailySnapshot);
    updateJobCard(elements.dailyIndicator, indicatorSnapshot);
    updateJobCard(elements.incomeStatement, incomeSnapshot);
    updateJobCard(elements.financialIndicator, financialSnapshot);
    updateJobCard(elements.financeBreakfast, breakfastSnapshot);

    if (data.config) {
      elements.config.includeSt.checked = !!data.config.includeST;
      elements.config.includeDelisted.checked = !!data.config.includeDelisted;
      elements.config.window.value = data.config.dailyTradeWindowDays ?? 420;
    }

    const shouldPoll = [
      stockSnapshot,
      dailySnapshot,
      indicatorSnapshot,
      incomeSnapshot,
      financialSnapshot,
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
    await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setTimeout(loadStatus, 500);
    if (!pollTimer) {
      pollTimer = setInterval(loadStatus, 3000);
    }
  } catch (error) {
    console.error("Failed to start job", error);
  }
}

async function saveConfig() {
  const payload = {
    includeST: elements.config.includeSt.checked,
    includeDelisted: elements.config.includeDelisted.checked,
    dailyTradeWindowDays: Number(elements.config.window.value) || 420,
  };
  try {
    await fetch(`${API_BASE}/control/config`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    alert(translations[currentLang].toastConfigSaved);
    loadStatus();
  } catch (error) {
    console.error("Failed to save config", error);
  }
}

function initLanguageSwitch() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLang(btn.dataset.lang))
  );
}

function initActions() {
  elements.stockBasic.button.addEventListener("click", () =>
    triggerJob("/control/sync/stock-basic", {})
  );
  elements.dailyTrade.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-trade", {
      window_days: Number(elements.config.window.value) || undefined,
    })
  );
  elements.dailyIndicator.button.addEventListener("click", () =>
    triggerJob("/control/sync/daily-indicators", {})
  );
  elements.incomeStatement.button.addEventListener("click", () =>
    triggerJob("/control/sync/income-statements", {})
  );
  elements.financialIndicator.button.addEventListener("click", () =>
    triggerJob("/control/sync/financial-indicators", {})
  );
  elements.financeBreakfast.button.addEventListener("click", () =>
    triggerJob("/control/sync/finance-breakfast", {})
  );
  elements.config.save.addEventListener("click", saveConfig);
}

// Boot
initLanguageSwitch();
initActions();
setLang(currentLang);
loadStatus();












