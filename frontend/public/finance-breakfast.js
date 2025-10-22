const translations = {
  en: {
    title: "Trend View – Daily Finance",
    brandName: "Trend View",
    brandTagline: "Investment Intelligence Hub",
    navBasics: "Basic Insights",
    navBasicInfo: "Basic Info",
    navNews: "Market News",
    navSignals: "Technical Signals",
    navPortfolio: "Portfolio Monitor",
    navControl: "Control Panel",
    navNewsGroup: "News",
    navDailyFinance: "Daily Finance",
    pageTitle: "Daily Finance",
    sectionTitle: "Morning Briefing",
    sectionSubtitle: "Start the trading day with curated highlights from Eastmoney.",
    emptyState: "No finance news available.",
    publishedAt: "Published",
    readMore: "Read more",
  },
  zh: {
    title: "趋势视图 - 每日财经",
    brandName: "趋势视图",
    brandTagline: "智能投研中心",
    navBasics: "基础洞察",
    navBasicInfo: "基础信息",
    navNews: "市场资讯",
    navSignals: "技术信号",
    navPortfolio: "组合监控",
    navControl: "控制面板",
    navNewsGroup: "资讯",
    navDailyFinance: "每日财经",
    pageTitle: "每日财经",
    sectionTitle: "财经早餐",
    sectionSubtitle: "东财财经早餐精选，开启你的一天。",
    emptyState: "暂无财经早餐内容。",
    publishedAt: "发布时间",
    readMore: "查看详情",
  },
};

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  container: document.getElementById("finance-list"),
};

const state = {
  entries: [],
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

  const prefAttribute = document.documentElement.getAttribute("data-pref-lang");
  if (prefAttribute && translations[prefAttribute]) {
    return prefAttribute;
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

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.title = dict.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    if (key && dict[key]) {
      el.textContent = dict[key];
    }
  });

  renderEntries(state.entries);
}

function formatDate(value) {
  if (!value) return "—";
  const dateValue = new Date(value);
  if (Number.isNaN(dateValue.getTime())) {
    return "—";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(dateValue);
}

function renderEntries(entries) {
  const dict = translations[currentLang];
  const container = elements.container;
  container.innerHTML = "";

  if (!entries || entries.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = dict.emptyState;
    container.appendChild(empty);
    return;
  }

  entries.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "card news-card";

    const titleLink = document.createElement("a");
    titleLink.className = "news-card__title";
    titleLink.textContent = entry.title || "";
    if (entry.url) {
      titleLink.href = entry.url;
      titleLink.target = "_blank";
      titleLink.rel = "noopener noreferrer";
    }

    const meta = document.createElement("div");
    meta.className = "news-card__meta";
    meta.textContent = `${dict.publishedAt}: ${formatDate(entry.published_at)}`;

    const summary = document.createElement("p");
    summary.className = "news-card__summary";
    summary.textContent = entry.summary || dict.emptyState;

    card.appendChild(titleLink);
    card.appendChild(meta);
    card.appendChild(summary);

    if (entry.url) {
      const link = document.createElement("a");
      link.className = "news-card__link";
      link.href = entry.url;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = dict.readMore;
      card.appendChild(link);
    }

    container.appendChild(card);
  });
}

async function loadEntries() {
  try {
    const response = await fetch(`${API_BASE}/finance-breakfast?limit=100`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    state.entries = Array.isArray(data)
      ? data.map((item) => ({
          title: item.title || "",
          summary: item.summary || "",
          published_at: item.publishedAt || item.published_at,
          url: item.url || "",
        }))
      : [];
  } catch (error) {
    console.error("Failed to load finance breakfast entries", error);
    state.entries = [];
  }

  renderEntries(state.entries);
}

elements.langButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    const lang = btn.dataset.lang;
    if (!lang || lang === currentLang || !translations[lang]) {
      return;
    }
    currentLang = lang;
    persistLanguage(lang);
    applyTranslations();
  });
});

persistLanguage(currentLang);
applyTranslations();
loadEntries();
