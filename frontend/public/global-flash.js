console.info("Global Flash bundle v20261108");

const translations = getTranslations("globalFlash");
const MAX_FETCH_ATTEMPTS = 3;
const FETCH_RETRY_BACKOFF_MS = 500;
const MAX_REFRESH_RETRIES = 3;
const REFRESH_RETRY_DELAY_MS = 5000;
const INVALID_SUMMARY_VALUES = new Set([
  "",
  "nan",
  "none",
  "null",
  "undefined",
  "-",
  "--",
]);

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  container: document.getElementById("global-flash-list"),
  tabs: document.querySelectorAll(".flash-tab"),
  rangeSelect: document.getElementById("flash-range-select"),
};

const state = {
  entries: [],
  activeTab: "high",
  dateRange: "all",
};

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function sanitizeSummary(value) {
  if (typeof value !== "string") {
    return "";
  }
  const compact = value.replace(/\s+/g, " ").trim();
  if (!compact) {
    return "";
  }
  return INVALID_SUMMARY_VALUES.has(compact.toLowerCase()) ? "" : compact;
}

function pickString(source, fields) {
  for (const field of fields) {
    if (!source || !(field in source)) {
      continue;
    }
    const candidate = source[field];
    if (typeof candidate === "string") {
      const trimmed = candidate.trim();
      if (trimmed) {
        return trimmed;
      }
    }
  }
  return "";
}

function normalizeEntry(raw) {
  const title = pickString(raw, ["title", "Title"]);
  const summary = sanitizeSummary(pickString(raw, ["summary", "Summary"]));
  const url = pickString(raw, ["url", "Url", "URL", "link", "Link"]);
  const publishedAt = pickString(raw, [
    "publishedAt",
    "published_at",
    "PublishedAt",
    "Published_at",
  ]);
  const rawImpact =
    raw.ifExtract ?? raw.if_extract ?? raw.impact ?? raw.Impact ?? null;
  let impact = null;
  if (typeof rawImpact === "boolean") {
    impact = rawImpact;
  } else if (typeof rawImpact === "string") {
    const flag = rawImpact.trim().toLowerCase();
    if (["true", "yes", "1", "y"].includes(flag)) {
      impact = true;
    } else if (["false", "no", "0", "n"].includes(flag)) {
      impact = false;
    }
  }
  const reasonRaw = pickString(raw, ["extractReason", "extract_reason"]);
  const extractReason = reasonRaw ? reasonRaw.replace(/\s+/g, " ").trim() : "";
  const extractCheckedAt = pickString(raw, [
    "extractCheckedAt",
    "extract_checked_at",
  ]);
  const subjectLevel = pickString(raw, [
    "subjectLevel",
    "subject_level",
    "SubjectLevel",
  ]);
  const impactScope = pickString(raw, [
    "impactScope",
    "impact_scope",
    "ImpactScope",
  ]);
  const eventType = pickString(raw, ["eventType", "event_type", "EventType"]);
  const timeSensitivity = pickString(raw, [
    "timeSensitivity",
    "time_sensitivity",
    "TimeSensitivity",
  ]);
  const quantSignal = pickString(raw, [
    "quantSignal",
    "quant_signal",
    "QuantSignal",
  ]);
  const impactLevels = normalizeArray(raw.impactLevels || raw.impact_levels || raw.ImpactLevels);
  const impactMarkets = normalizeArray(raw.impactMarkets || raw.impact_markets);
  const impactIndustries = normalizeArray(raw.impactIndustries || raw.impact_industries);
  const impactSectors = normalizeArray(raw.impactSectors || raw.impact_sectors);
  const impactThemes = normalizeArray(raw.impactThemes || raw.impact_themes);
  const impactStocks = normalizeArray(raw.impactStocks || raw.impact_stocks);
  return {
    title,
    summary,
    publishedAt,
    url,
    impact,
    extractReason,
    extractCheckedAt,
    subjectLevel,
    impactScope,
    eventType,
    timeSensitivity,
    quantSignal,
    impactLevels,
    impactMarkets,
    impactIndustries,
    impactSectors,
    impactThemes,
    impactStocks,
    raw,
  };
}

function normalizeArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];
    if ((trimmed.startsWith("[") && trimmed.endsWith("]")) || (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          return parsed.map((item) => String(item).trim()).filter(Boolean);
        }
      } catch (error) {
        // fall back to splitting
      }
    }
    return trimmed
      .split(/[,，\/]/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

function filterEntries(entries) {
  if (!Array.isArray(entries)) {
    return [];
  }
  const dateFiltered = entries.filter((entry) => matchesDateRange(entry.publishedAt, state.dateRange));
  if (state.activeTab === "low") {
    return dateFiltered.filter((entry) => entry.impact === false);
  }
  if (state.activeTab === "unknown") {
    return dateFiltered.filter((entry) => entry.impact == null);
  }
  return dateFiltered.filter((entry) => entry.impact === true);
}

function matchesDateRange(dateValue, rangeKey) {
  if (!dateValue || rangeKey === "all") {
    return true;
  }
  const published = new Date(dateValue);
  if (Number.isNaN(published.getTime())) {
    return false;
  }
  const now = new Date();
  const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  switch (rangeKey) {
    case "today":
      return published >= startOfToday;
    case "3d": {
      const start = new Date(startOfToday);
      start.setDate(start.getDate() - 2);
      return published >= start;
    }
    case "week": {
      const start = new Date(startOfToday);
      start.setDate(start.getDate() - 6);
      return published >= start;
    }
    case "month": {
      const start = new Date(startOfToday);
      start.setDate(start.getDate() - 29);
      return published >= start;
    }
    default:
      return true;
  }
}

async function fetchWithRetry(url, options = {}) {
  let lastError;
  for (let attempt = 0; attempt < MAX_FETCH_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetch(url, options);
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      return response;
    } catch (error) {
      lastError = error;
      if (attempt < MAX_FETCH_ATTEMPTS - 1) {
        // Linear backoff to avoid hammering backend on transient failures.
        await sleep(FETCH_RETRY_BACKOFF_MS * (attempt + 1));
      }
    }
  }
  throw lastError;
}

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    // ignore storage errors
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

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    // ignore storage errors
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

let currentLang = getInitialLanguage();
persistLanguage(currentLang);

function formatDate(value) {
  if (!value) return "--";
  const dateValue = new Date(value);
  if (Number.isNaN(dateValue.getTime())) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(dateValue);
}

function createCard(entry) {
  const dict = translations[currentLang];
  const card = document.createElement("article");
  card.className = "news-card";

  if (entry.impact === true) {
    card.classList.add("news-card--impact-positive");
    const badge = document.createElement("span");
    badge.className = "news-card__badge news-card__badge--positive";
    badge.textContent = dict.impactBadge || "Market Focus";
    card.appendChild(badge);
  } else if (entry.impact === false) {
    card.classList.add("news-card--impact-negative");
    const badge = document.createElement("span");
    badge.className = "news-card__badge news-card__badge--neutral";
    badge.textContent = dict.impactBadgeNegative || "Reviewed";
    card.appendChild(badge);
  }

  const title = document.createElement("h2");
  title.className = "news-card__title";
  title.textContent = entry.title || "--";
  card.appendChild(title);

  if (entry.summary) {
    const summary = document.createElement("p");
    summary.className = "news-card__summary";
    summary.textContent = entry.summary;
    card.appendChild(summary);
  }

  const meta = document.createElement("div");
  meta.className = "news-card__meta";
  meta.textContent = `${dict.publishedAt || "Published"}: ${formatDate(entry.publishedAt)}`;
  card.appendChild(meta);

  if (entry.url) {
    const link = document.createElement("a");
    link.className = "news-card__link";
    link.href = entry.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = dict.readMore || "View source";
    card.appendChild(link);
  }

  const hasInsight =
    entry.extractReason ||
    entry.subjectLevel ||
    entry.impactScope ||
    entry.eventType ||
    entry.timeSensitivity ||
    entry.quantSignal ||
    (entry.impactLevels && entry.impactLevels.length) ||
    (entry.impactMarkets && entry.impactMarkets.length) ||
    (entry.impactIndustries && entry.impactIndustries.length) ||
    (entry.impactSectors && entry.impactSectors.length) ||
    (entry.impactThemes && entry.impactThemes.length) ||
    (entry.impactStocks && entry.impactStocks.length) ||
    entry.impact !== null;

  if (hasInsight) {
    const insight = document.createElement("section");
    insight.className = "news-card__analysis";

    const header = document.createElement("h3");
    header.className = "news-card__analysis-title";
    header.textContent = dict.aiInsightTitle || "AI Insight";
    insight.appendChild(header);

    if (entry.extractReason) {
      const reason = document.createElement("p");
      reason.className = "news-card__impact-reason";
      const label = dict.impactReasonLabel || "Reason";
      reason.textContent = `${label}: ${entry.extractReason}`;
      insight.appendChild(reason);
    }

    const levelLabelMap = {
      market: dict.impactLevelMarket || "Market",
      industry: dict.impactLevelIndustry || "Industry",
      sector: dict.impactLevelSector || "Sector",
      theme: dict.impactLevelTheme || "Theme",
      stock: dict.impactLevelStock || "Stock",
    };
    const levelValues = (entry.impactLevels || []).map((code) => levelLabelMap[code] || code);
    if (levelValues.length) {
      const levelRow = document.createElement("div");
      levelRow.className = "news-card__levels";
      levelValues.forEach((value) => {
        const chip = document.createElement("span");
        chip.className = "news-card__level-chip";
        chip.textContent = value;
        levelRow.appendChild(chip);
      });
      insight.appendChild(levelRow);
    }

    const list = document.createElement("dl");
    list.className = "news-card__analysis-list";

    const impactLabel = dict.aiImpactLabel || "Impact";
    let impactValue = dict.aiImpactUnknown || "Unknown";
    if (entry.impact === true) {
      impactValue = dict.aiImpactPositive || "Significant";
    } else if (entry.impact === false) {
      impactValue = dict.aiImpactNegative || "Limited";
    }

    const rows = [
      { label: impactLabel, value: impactValue },
      { label: dict.aiSubjectLabel || "Subject Level", value: entry.subjectLevel },
      { label: dict.aiScopeLabel || "Impact Scope", value: entry.impactScope },
      { label: dict.aiEventLabel || "Event Type", value: entry.eventType },
      { label: dict.aiTimeLabel || "Time Sensitivity", value: entry.timeSensitivity },
      { label: dict.aiQuantLabel || "Quant Signal", value: entry.quantSignal },
    ];

    rows
      .filter((row) => row.value && typeof row.value === "string" && row.value.trim())
      .forEach((row) => {
        const item = document.createElement("div");
        item.className = "news-card__analysis-item";

        const term = document.createElement("dt");
        term.className = "news-card__analysis-term";
        term.textContent = row.label;

        const desc = document.createElement("dd");
        desc.className = "news-card__analysis-desc";
        desc.textContent = row.value.trim();

        item.appendChild(term);
        item.appendChild(desc);
        list.appendChild(item);
      });

    if (list.children.length > 0) {
      insight.appendChild(list);
    }

    const detailGroups = [
      { label: dict.aiMarketsLabel || "Markets", items: entry.impactMarkets },
      { label: dict.aiIndustriesLabel || "Industries", items: entry.impactIndustries },
      { label: dict.aiSectorsLabel || "Sectors", items: entry.impactSectors },
      { label: dict.aiThemesLabel || "Themes", items: entry.impactThemes },
      { label: dict.aiStocksLabel || "Stocks", items: entry.impactStocks },
    ];

    detailGroups
      .filter((group) => Array.isArray(group.items) && group.items.length)
      .forEach((group) => {
        const wrapper = document.createElement("div");
        wrapper.className = "news-card__chip-group";

        const label = document.createElement("span");
        label.className = "news-card__chip-label";
        label.textContent = group.label;
        wrapper.appendChild(label);

        const listEl = document.createElement("div");
        listEl.className = "news-card__chip-list";
        group.items.forEach((item) => {
          const chip = document.createElement("span");
          chip.className = "news-card__chip";
          chip.textContent = item;
          listEl.appendChild(chip);
        });
        wrapper.appendChild(listEl);
        insight.appendChild(wrapper);
      });

    card.appendChild(insight);
  }

  return card;
}

function renderEntries(entries) {
  if (!elements.container) {
    return;
  }
  elements.container.innerHTML = "";
  updateTabCounts(entries);
  const filtered = filterEntries(entries);
  if (!filtered || !filtered.length) {
    const message = elements.container.dataset[
      `empty${currentLang.toUpperCase()}`
    ] || "";
    const placeholder = document.createElement("div");
    placeholder.className = "empty-placeholder";
    placeholder.textContent = message || "--";
    elements.container.appendChild(placeholder);
    return;
  }

  const fragment = document.createDocumentFragment();
  filtered.forEach((entry) => {
    fragment.appendChild(createCard(entry));
  });
  elements.container.appendChild(fragment);
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.title = dict.title;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  document.querySelectorAll("[data-i18n-option]").forEach((option) => {
    const key = option.dataset.i18nOption;
    if (key && dict[key]) {
      option.textContent = dict[key];
    }
  });
  if (elements.rangeSelect) {
    elements.rangeSelect.value = state.dateRange;
  }
  renderEntries(state.entries);
}

function setActiveTab(tabKey) {
  if (!tabKey || state.activeTab === tabKey) {
    return;
  }
  state.activeTab = tabKey;
  elements.tabs.forEach((tab) => {
    const isActive = tab.dataset.tab === tabKey;
    tab.classList.toggle("flash-tab--active", isActive);
    tab.setAttribute("aria-selected", String(isActive));
  });
  renderEntries(state.entries);
}

function updateTabCounts(entries) {
  if (!elements.tabs) {
    return;
  }
  const base = Array.isArray(entries) ? entries : [];
  const byDate = base.filter((entry) => matchesDateRange(entry.publishedAt, state.dateRange));
  const counts = {
    high: byDate.filter((entry) => entry.impact === true).length,
    low: byDate.filter((entry) => entry.impact === false).length,
    unknown: byDate.filter((entry) => entry.impact == null).length,
  };
  elements.tabs.forEach((tab) => {
    const bucket = tab.dataset.tab;
    const value = counts[bucket] ?? 0;
    const indicator = tab.querySelector(".flash-tab__count");
    if (indicator) {
      indicator.textContent = `(${value})`;
    }
  });
}

async function loadEntries(options = {}) {
  const { attempt = 0 } = options;
  const requestUrl = `${API_BASE}/news/global-flash?limit=200`;
  try {
    const response = await fetchWithRetry(requestUrl);
    const data = await response.json();
    const entries = Array.isArray(data)
      ? data
          .map((item) => normalizeEntry(item))
          .filter((entry) => entry.title && entry.publishedAt && entry.url)
      : [];
    state.entries = entries;
    renderEntries(state.entries);
    if (!state.entries.length && attempt < MAX_REFRESH_RETRIES) {
      window.setTimeout(
        () => loadEntries({ attempt: attempt + 1 }),
        REFRESH_RETRY_DELAY_MS
      );
    }
  } catch (error) {
    console.error("Failed to load global flash entries", error);
    state.entries = [];
    renderEntries(state.entries);
  }
}

function initLanguageSwitcher() {
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
    btn.addEventListener("click", () => {
      if (!btn.dataset.lang || btn.dataset.lang === currentLang) {
        return;
      }
      currentLang = btn.dataset.lang;
      persistLanguage(currentLang);
      applyTranslations();
      elements.langButtons.forEach((other) => {
        other.classList.toggle("active", other.dataset.lang === currentLang);
      });
    });
  });
}

function initRangeSelect() {
  if (!elements.rangeSelect) {
    return;
  }
  elements.rangeSelect.value = state.dateRange;
  elements.rangeSelect.addEventListener("change", () => {
    state.dateRange = elements.rangeSelect.value || "all";
    updateTabCounts(state.entries);
    renderEntries(state.entries);
  });
}

function initTabs() {
  if (!elements.tabs || !elements.tabs.length) {
    return;
  }
  elements.tabs.forEach((tab) => {
    tab.classList.toggle("flash-tab--active", tab.dataset.tab === state.activeTab);
    tab.setAttribute("aria-selected", String(tab.dataset.tab === state.activeTab));
    tab.addEventListener("click", () => {
      setActiveTab(tab.dataset.tab);
    });
  });
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
applyTranslations();
initLanguageSwitcher();
initTabs();
initRangeSelect();
loadEntries();
