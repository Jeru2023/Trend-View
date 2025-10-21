const translations = {
  en: {
    title: "Trend View – Control Panel",
    brandName: "Trend View",
    brandTagline: "Investment Intelligence Hub",
    navBasics: "Basic Insights",
    navBasicInfo: "Basic Info",
    navNews: "Market News",
    navSignals: "Technical Signals",
    navPortfolio: "Portfolio Monitor",
    navControl: "Control Panel",
    pageTitle: "Control Panel",
    syncSectionTitle: "Data Synchronization",
    syncSectionSubtitle: "Trigger updates manually and monitor automated job status in real time.",
    stockBasicTitle: "Stock Basic Data",
    stockBasicSubtitle: "Monthly refresh (1st of each month) or run manually as needed.",
    dailyTradeTitle: "Daily Trade Data",
    dailyTradeSubtitle: "Scheduled daily at 17:00; follow batch progress when running.",
    runNow: "Run Now",
    lastStatus: "Status",
    lastUpdated: "Last Updated",
    lastDuration: "Last Duration",
    records: "Records",
    configSectionTitle: "Configuration",
    configSectionSubtitle: "Adjust query filters and default window sizes for automated jobs.",
    includeStLabel: "Include ST stocks in queries",
    includeStHint: "When enabled, ST/*ST securities appear in list results.",
    includeDelistedLabel: "Include delisted stocks",
    includeDelistedHint: "When disabled, delisted/paused securities are hidden.",
    windowLabel: "Daily trade window (days)",
    windowHint: "Used by scheduled and manual daily trade updates.",
    saveSettings: "Save Settings",
    statusIdle: "Idle",
    statusRunning: "Running",
    statusSuccess: "Completed",
    statusFailed: "Failed",
    statusUnknown: "Unknown",
    messageNone: "Awaiting next run.",
    toastConfigSaved: "Configuration updated",
  },
  zh: {
    title: "趋势视图 - 控制面板",
    brandName: "趋势视图",
    brandTagline: "智能投研中心",
    navBasics: "基础洞察",
    navBasicInfo: "基础信息",
    navNews: "市场资讯",
    navSignals: "技术信号",
    navPortfolio: "组合监控",
    navControl: "控制面板",
    pageTitle: "控制面板",
    syncSectionTitle: "数据同步",
    syncSectionSubtitle: "手动触发更新，并实时查看自动任务的执行状态。",
    stockBasicTitle: "股票基础数据",
    stockBasicSubtitle: "每月1日自动刷新，可手动执行。",
    dailyTradeTitle: "日交易数据",
    dailyTradeSubtitle: "每日17:00自动更新，可查看批次进度。",
    runNow: "立即执行",
    lastStatus: "当前状态",
    lastUpdated: "上次更新时间",
    lastDuration: "上次耗时",
    records: "记录数",
    configSectionTitle: "配置项",
    configSectionSubtitle: "调整筛选开关以及自动任务的历史窗口。",
    includeStLabel: "查询结果包含 ST 股票",
    includeStHint: "开启后，ST/*ST 股票将显示在列表中。",
    includeDelistedLabel: "查询结果包含退市股票",
    includeDelistedHint: "关闭后，将隐藏退市/停牌股票。",
    windowLabel: "日交易更新历史天数",
    windowHint: "影响自动任务与手动任务的抓取周期。",
    saveSettings: "保存设置",
    statusIdle: "空闲",
    statusRunning: "执行中",
    statusSuccess: "已完成",
    statusFailed: "失败",
    statusUnknown: "未知",
    messageNone: "等待执行。",
    toastConfigSaved: "配置已保存",
  },
};

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";

function getInitialLanguage() {
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  const htmlLang = document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) {
    return htmlLang;
  }
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    /* no-op */
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

function formatNumber(value) {
  if (value === null || value === undefined) return "—";
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale).format(value);
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

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)} ms`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins === 0) return `${secs.toFixed(1)} s`;
  return `${mins}m ${secs.toFixed(0)}s`;
}

function updateJobCard(cardElements, snapshot) {
  cardElements.status.textContent = jobStatusLabel(snapshot.status);
  cardElements.updated.textContent = formatDateTime(
    snapshot.finishedAt || snapshot.startedAt
  );
  if (cardElements.duration) {
    const durationValue =
      snapshot.lastDuration ?? snapshot.lastMarket ?? null;
    cardElements.duration.textContent = formatDuration(durationValue);
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

    updateJobCard(elements.stockBasic, stockSnapshot);
    updateJobCard(elements.dailyTrade, dailySnapshot);

    if (data.config) {
      elements.config.includeSt.checked = !!data.config.includeST;
      elements.config.includeDelisted.checked = !!data.config.includeDelisted;
      elements.config.window.value = data.config.dailyTradeWindowDays ?? 420;
    }

    const shouldPoll =
      stockSnapshot.status === "running" || dailySnapshot.status === "running";
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
  elements.config.save.addEventListener("click", saveConfig);
}

// Boot
initLanguageSwitch();
initActions();
setLang(currentLang);
loadStatus();
