console.info("Finance Breakfast bundle v20261110");

const translations = getTranslations("financeBreakfast");

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
    // ignore storage errors
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
  if (browserLang.startsWith("zh") && translations.zh) {
    return "zh";
  }
  if (browserLang.startsWith("en") && translations.en) {
    return "en";
  }
  return "zh";
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

function missingValue() {
  return currentLang === "zh" ? "--" : "--";
}

function sanitizeText(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value.replace(/\s+/g, " ").trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value
      .map((item) => sanitizeText(item))
      .filter(Boolean)
      .join("、");
  }
  if (typeof value === "object") {
    try {
      const text = JSON.stringify(value);
      return text === "{}" ? "" : text;
    } catch (error) {
      return String(value).trim();
    }
  }
  return String(value).trim();
}

function formatDate(value) {
  if (!value) return missingValue();
  const dateValue = new Date(value);
  if (Number.isNaN(dateValue.getTime())) {
    return missingValue();
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(dateValue);
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
    } else if (typeof candidate === "number" && Number.isFinite(candidate)) {
      return String(candidate);
    }
  }
  return "";
}

function normalizeArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) {
    return [...new Set(value.map((item) => sanitizeText(item)).filter(Boolean))];
  }
  const text = sanitizeText(value);
  if (!text) {
    return [];
  }
  return [...new Set(text.split(/[,，;；、\n]+/).map((item) => item.trim()).filter(Boolean))];
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

function normalizeEntry(raw) {
  const relevance = (raw && raw.relevance) || {};
  const impactInfo = (raw && raw.impact) || {};
  const metadata = parseMetadata(impactInfo.metadata);

  const title = pickString(raw, ["title"]);
  let summary = sanitizeText(pickString(raw, ["summary"]));
  if (!summary) {
    summary = sanitizeText(pickString(impactInfo, ["summary", "impact_summary"]));
  }
  const content = sanitizeText(pickString(raw, ["content"]));
  const url = pickString(raw, ["url"]);
  const publishedAt = pickString(raw, ["publishedAt", "published_at"]);

  let impactLevels = normalizeArray(impactInfo.levels || impactInfo.impactLevels);
  let impactIndustries = normalizeArray(impactInfo.industries || impactInfo.impactIndustries);
  let impactSectors = normalizeArray(impactInfo.sectors || impactInfo.impactSectors);
  let impactThemes = normalizeArray(impactInfo.themes || impactInfo.impactThemes);
  let impactStocks = normalizeArray(impactInfo.stocks || impactInfo.impactStocks);

  const subjectLevel = metadata ? pickString(metadata, ["subject_level", "subjectLevel"]) : "";
  const impactScope = metadata
    ? pickString(metadata, ["impact_scope", "impactScope"]) ||
      (Array.isArray(metadata.impact_scope_levels)
        ? metadata.impact_scope_levels.map((item) => String(item).trim()).filter(Boolean).join("、")
        : "")
    : "";
  const eventType = metadata ? pickString(metadata, ["event_type", "eventType"]) : "";
  const timeSensitivity = metadata ? pickString(metadata, ["time_sensitivity", "timeSensitivity"]) : "";
  const quantSignal = metadata ? pickString(metadata, ["quant_signal", "quantSignal"]) : "";
  const focusTopics = metadata ? normalizeArray(metadata.focus_topics) : [];

  if (metadata && metadata.impact_scope_details) {
    const details = metadata.impact_scope_details;
    if (details && typeof details === "object") {
      impactIndustries = mergeUnique(impactIndustries, normalizeArray(details.industry || details["行业"]));
      impactSectors = mergeUnique(impactSectors, normalizeArray(details.sector || details["板块"]));
      impactThemes = mergeUnique(impactThemes, normalizeArray(details.theme || details["概念"] || details["题材"]));
      impactStocks = mergeUnique(impactStocks, normalizeArray(details.stock || details["个股"] || details["公司"]));
    }
  }

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
  const extractReason = sanitizeText(pickString(relevance, ["reason", "relevance_reason"]));
  const extractCheckedAt = pickString(relevance, ["checkedAt", "checked_at", "relevance_checked_at"]);

  const impactConfidence =
    typeof impactInfo.impact_confidence === "number"
      ? impactInfo.impact_confidence
      : typeof impactInfo.confidence === "number"
      ? impactInfo.confidence
      : null;
  const impactSummary = sanitizeText(pickString(impactInfo, ["impact_summary", "summary"]));
  const impactAnalysis = sanitizeText(pickString(impactInfo, ["impact_analysis", "analysis"]));

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
    impactIndustries,
    impactSectors,
    impactThemes,
    impactStocks,
    impactSummary,
    impactAnalysis,
    impactConfidence,
    raw,
  };
}

function createCard(entry) {
  const dict = translations[currentLang];
  const card = document.createElement("article");
  card.className = "news-card";

  const header = document.createElement("div");
  header.className = "news-card__header";

  if (entry.title) {
    const title = document.createElement("h2");
    title.className = "news-card__title";
    title.textContent = entry.title;
    header.appendChild(title);
  }

  if (header.children.length) {
    card.appendChild(header);
  }

  if (entry.summary) {
    const summary = document.createElement("p");
    summary.className = "news-card__summary";
    summary.textContent = entry.summary;
    card.appendChild(summary);
  }

  if (entry.content) {
    const content = document.createElement("p");
    content.className = "news-card__content";
    content.textContent = entry.content;
    card.appendChild(content);
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

    const rows = [
      {
        label: dict.aiImpactLabel || "Impact",
        value:
          entry.impact === true
            ? dict.aiImpactPositive || "Material"
            : entry.impact === false
            ? dict.aiImpactNegative || "Limited"
            : dict.aiImpactUnknown || "Unknown",
      },
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
      {
        label: dict.aiFocusTopicsLabel || "Focus Topics",
        value: entry.focusTopics && entry.focusTopics.length ? entry.focusTopics.join("、") : "",
      },
    ];

    const list = document.createElement("dl");
    list.className = "news-card__analysis-list";
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

    if (list.children.length) {
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
      { label: dict.aiStocksLabel || "Stocks", items: entry.impactStocks },
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
  const list = Array.isArray(entries) ? entries : [];
  if (!list.length) {
    const message = elements.container.dataset[`empty${currentLang.toUpperCase()}`] || missingValue();
    const placeholder = document.createElement("div");
    placeholder.className = "empty-placeholder";
    placeholder.textContent = message;
    elements.container.appendChild(placeholder);
    return;
  }
  const fragment = document.createDocumentFragment();
  list.forEach((entry) => fragment.appendChild(createCard(entry)));
  elements.container.appendChild(fragment);
}

async function loadEntries() {
  const requestUrl = `${API_BASE}/finance-breakfast?limit=50`;
  try {
    const response = await fetch(requestUrl);
    if (!response.ok) {
      throw new Error(`Finance breakfast fetch failed: ${response.status}`);
    }
    const data = await response.json();
    const entries = Array.isArray(data)
      ? data
          .map((item) => normalizeEntry(item))
          .filter((entry) => entry.title && entry.publishedAt)
      : [];
    state.entries = entries;
    renderEntries(state.entries);
  } catch (error) {
    console.error("Failed to load finance breakfast entries", error);
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
  renderEntries(state.entries);
}

initLanguageSwitcher();
applyTranslations();
loadEntries();
