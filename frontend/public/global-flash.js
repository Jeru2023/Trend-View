console.info("Global Flash bundle v20261110");

const translations = getTranslations("globalFlash");
const MAX_FETCH_ATTEMPTS = 3;
const FETCH_RETRY_BACKOFF_MS = 500;
const MAX_REFRESH_RETRIES = 3;
const REFRESH_RETRY_DELAY_MS = 5000;
const GLOBAL_FLASH_FETCH_LIMIT = 500;
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
    } else if (Array.isArray(candidate)) {
      const joined = candidate
        .map((item) => String(item).trim())
        .filter(Boolean)
        .join("、");
      if (joined) {
        return joined;
      }
    } else if (typeof candidate === "number" && !Number.isNaN(candidate)) {
      return String(candidate);
    }
  }
  return "";
}

function normalizeEntry(raw) {
  const title = pickString(raw, ["title", "Title"]);
  let summary = sanitizeSummary(pickString(raw, ["summary", "Summary"]));
  const content = pickString(raw, ["content", "Content"]);
  const url = pickString(raw, ["url", "Url", "URL", "link", "Link"]);
  const publishedAt = pickString(raw, [
    "publishedAt",
    "published_at",
    "PublishedAt",
    "Published_at",
  ]);

  const relevance = (raw && typeof raw === "object" && raw.relevance) || {};
  const impactInfo = (raw && typeof raw === "object" && raw.impact) || {};
  const metadata = parseMetadata(impactInfo.metadata);

  if (!summary) {
    summary = sanitizeSummary(pickString(impactInfo, ["summary", "impact_summary"]));
  }

  let impactLevels = normalizeArray(impactInfo.levels || impactInfo.impactLevels || impactInfo.impact_levels);
  let impactMarkets = normalizeArray(impactInfo.markets || impactInfo.impactMarkets || impactInfo.impact_markets);
  let impactIndustries = normalizeArray(
    impactInfo.industries || impactInfo.impactIndustries || impactInfo.impact_industries,
  );
  let impactSectors = normalizeArray(impactInfo.sectors || impactInfo.impactSectors || impactInfo.impact_sectors);
  let impactThemes = normalizeArray(impactInfo.themes || impactInfo.impactThemes || impactInfo.impact_themes);
  let impactStocks = normalizeArray(impactInfo.stocks || impactInfo.impactStocks || impactInfo.impact_stocks);

  if (metadata && typeof metadata === "object") {
    const scopeDetails = metadata.impact_scope_details;
    if (scopeDetails && typeof scopeDetails === "object") {
      impactMarkets = mergeUnique(impactMarkets, normalizeArray(scopeDetails.market || scopeDetails["大盘"]));
      impactIndustries = mergeUnique(impactIndustries, normalizeArray(scopeDetails.industry || scopeDetails["行业"]));
      impactSectors = mergeUnique(impactSectors, normalizeArray(scopeDetails.sector || scopeDetails["板块"]));
      impactThemes = mergeUnique(
        impactThemes,
        normalizeArray(scopeDetails.theme || scopeDetails["概念"] || scopeDetails["题材"]),
      );
      impactStocks = mergeUnique(impactStocks, normalizeArray(scopeDetails.stock || scopeDetails["个股"] || scopeDetails["公司"]));
    }
  }

  const impactScope = metadata
    ? pickString(metadata, ["impact_scope", "impactScope"]) ||
      (Array.isArray(metadata.impact_scope_levels)
        ? metadata.impact_scope_levels.map((item) => String(item).trim()).filter(Boolean).join("、")
        : "")
    : "";

  const subjectLevel = metadata ? pickString(metadata, ["subject_level", "subjectLevel"]) : "";
  const eventType = metadata ? pickString(metadata, ["event_type", "eventType"]) : "";
  const timeSensitivity = metadata ? pickString(metadata, ["time_sensitivity", "timeSensitivity"]) : "";
  const quantSignal = metadata ? pickString(metadata, ["quant_signal", "quantSignal"]) : "";
  const focusTopicsValue = metadata && metadata.focus_topics;
  const focusTopics = Array.isArray(focusTopicsValue)
    ? focusTopicsValue.map((item) => String(item).trim()).filter(Boolean)
    : normalizeArray(focusTopicsValue);

  let impact = null;
  if (typeof relevance.isRelevant === "boolean") {
    impact = relevance.isRelevant;
  } else if (typeof relevance.is_relevant === "boolean") {
    impact = relevance.is_relevant;
  }

  const relevanceConfidence =
    typeof relevance.relevance_confidence === "number"
      ? relevance.relevance_confidence
      : typeof relevance.confidence === "number"
      ? relevance.confidence
      : null;
  const extractReason = sanitizeSummary(pickString(relevance, ["reason", "relevance_reason"]));
  const extractCheckedAt = pickString(relevance, ["checkedAt", "checked_at", "relevance_checked_at"]);

  const impactConfidence =
    typeof impactInfo.impact_confidence === "number"
      ? impactInfo.impact_confidence
      : typeof impactInfo.confidence === "number"
      ? impactInfo.confidence
      : null;
  const impactCheckedAt = pickString(impactInfo, ["impact_checked_at", "checked_at", "checkedAt"]);
  const impactSummary = sanitizeSummary(pickString(impactInfo, ["impact_summary", "summary"]));
  const impactAnalysis = sanitizeSummary(pickString(impactInfo, ["impact_analysis", "analysis"]));

  return {
    id: raw.articleId || raw.article_id || "",
    source: raw.source || "",
    title,
    summary,
    content,
    publishedAt,
    url,
    impact,
    relevanceConfidence,
    extractReason,
    extractCheckedAt,
    subjectLevel,
    impactScope,
    eventType,
    timeSensitivity,
    quantSignal,
    focusTopics,
    impactLevels,
    impactMarkets,
    impactIndustries,
    impactSectors,
    impactThemes,
    impactStocks,
    impactSummary,
    impactAnalysis,
    impactConfidence,
    impactCheckedAt,
    raw,
  };
}

function normalizeArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) {
    return [...new Set(value.map((item) => String(item).trim()).filter(Boolean))];
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
    return [...new Set(trimmed
      .split(/[,，\/]/)
      .map((item) => item.trim())
      .filter(Boolean))];
  }
  return [];
}

function mergeUnique(base, additions) {
  const combined = Array.isArray(base) ? [...base] : [];
  (additions || []).forEach((item) => {
    const value = String(item).trim();
    if (value && !combined.includes(value)) {
      combined.push(value);
    }
  });
  return combined;
}

function parseMetadata(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    try {
      const parsed = JSON.parse(trimmed);
      if (parsed && typeof parsed === "object") {
        return parsed;
      }
    } catch (error) {
      console.debug("Failed to parse metadata JSON", error);
    }
  }
  return null;
}

function formatConfidence(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "";
  }
  const percentage = Math.round(value * 100);
  return `${percentage}%`;
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

  const header = document.createElement("div");
  header.className = "news-card__header";

  if (entry.impact === true) {
    card.classList.add("news-card--impact-positive");
    const badge = document.createElement("span");
    badge.className = "news-card__badge news-card__badge--positive";
    badge.textContent = dict.impactBadge || "Market Focus";
    header.appendChild(badge);
  } else if (entry.impact === false) {
    card.classList.add("news-card--impact-negative");
    const badge = document.createElement("span");
    badge.className = "news-card__badge news-card__badge--neutral";
    badge.textContent = dict.impactBadgeNegative || "Reviewed";
    header.appendChild(badge);
  }

  const title = document.createElement("h2");
  title.className = "news-card__title";
  title.textContent = entry.title || "--";
  header.appendChild(title);

  card.appendChild(header);

  if (entry.summary) {
    const summary = document.createElement("p");
    summary.className = "news-card__summary";
    summary.textContent = entry.summary;
    card.appendChild(summary);
  }

  const footer = document.createElement("div");
  footer.className = "news-card__footer";

  const meta = document.createElement("span");
  meta.className = "news-card__meta";
  meta.textContent = `${dict.publishedAt || "Published"}: ${formatDate(entry.publishedAt)}`;
  footer.appendChild(meta);

  if (entry.url) {
    const link = document.createElement("a");
    link.className = "news-card__link";
    link.href = entry.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = dict.readMore || "View source";
    footer.appendChild(link);
  }

  card.appendChild(footer);

  const hasInsight =
    entry.extractReason ||
    entry.subjectLevel ||
    entry.impactScope ||
    entry.eventType ||
    entry.timeSensitivity ||
    entry.quantSignal ||
    entry.impactSummary ||
    entry.impactAnalysis ||
    formatConfidence(entry.relevanceConfidence) ||
    formatConfidence(entry.impactConfidence) ||
    (entry.focusTopics && entry.focusTopics.length) ||
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

    if (entry.impactSummary) {
      const summary = document.createElement("p");
      summary.className = "news-card__impact-summary";
      summary.textContent = entry.impactSummary;
      insight.appendChild(summary);
    }

    if (entry.impactAnalysis) {
      const analysis = document.createElement("p");
      analysis.className = "news-card__impact-analysis";
      analysis.textContent = entry.impactAnalysis;
      insight.appendChild(analysis);
    }

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
      {
        label: dict.aiRelevanceConfidenceLabel || "Relevance Confidence",
        value: formatConfidence(entry.relevanceConfidence),
      },
      {
        label: dict.aiImpactConfidenceLabel || "Impact Confidence",
        value: formatConfidence(entry.impactConfidence),
      },
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

    const buildChipGroup = (labelText, items) => {
      if (!Array.isArray(items) || !items.length) {
        return null;
      }
      const wrapper = document.createElement("div");
      wrapper.className = "news-card__chip-group";

      const label = document.createElement("span");
      label.className = "news-card__chip-label";
      label.textContent = labelText;
      wrapper.appendChild(label);

      const listEl = document.createElement("div");
      listEl.className = "news-card__chip-list";
      items.forEach((item) => {
        const chip = document.createElement("span");
        chip.className = "news-card__chip";
        chip.textContent = item;
        listEl.appendChild(chip);
      });
      wrapper.appendChild(listEl);
      return wrapper;
    };

    const industryRowGroups = [
      { label: dict.aiIndustriesLabel || "Industries", items: entry.impactIndustries },
      { label: dict.aiSectorsLabel || "Sectors", items: entry.impactSectors },
      { label: dict.aiThemesLabel || "Themes", items: entry.impactThemes },
    ].map((group) => buildChipGroup(group.label, group.items)).filter(Boolean);

    if (industryRowGroups.length) {
      const row = document.createElement("div");
      row.className = "news-card__chip-row";
      industryRowGroups.forEach((group) => row.appendChild(group));
      insight.appendChild(row);
    }

    [
      { label: dict.aiMarketsLabel || "Markets", items: entry.impactMarkets },
      { label: dict.aiStocksLabel || "Stocks", items: entry.impactStocks },
      { label: dict.aiFocusTopicsLabel || "Focus Topics", items: entry.focusTopics },
    ]
      .map((group) => buildChipGroup(group.label, group.items))
      .filter(Boolean)
      .forEach((group) => insight.appendChild(group));

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
  const requestUrl = `${API_BASE}/news/global-flash?limit=${GLOBAL_FLASH_FETCH_LIMIT}`;
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
