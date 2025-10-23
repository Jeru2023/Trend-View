console.info("Finance Breakfast bundle v20251026")
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
  return browserLang.startsWith("zh") ? "zh" : "en";
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

function cleanText(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value
      .map((item) => cleanText(item))
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

function toList(value) {
  if (value === null || value === undefined) {
    return [];
  }
  if (Array.isArray(value)) {
    return value.map((item) => cleanText(item)).filter(Boolean);
  }
  const text = cleanText(value);
  if (!text) {
    return [];
  }
  return text
    .split(/[,，;；、\n]+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseIntensity(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  const text = cleanText(value).replace(/[^0-9.+-]+/g, "");
  if (!text) {
    return null;
  }
  const parsed = Number.parseFloat(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function getFirstMatchingProperty(obj, keys) {
  if (!obj || typeof obj !== "object") {
    return undefined;
  }
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key) && obj[key] != null) {
      return obj[key];
    }
  }
  return undefined;
}

function hasAiMarkers(obj) {
  if (!obj || typeof obj !== "object") {
    return false;
  }
  const aiKeys = [
    "ImpactAnalysis",
    "impactAnalysis",
    "ComprehensiveAssessment",
    "影响事项",
    "影響事項",
    "impact_items",
    "impactEvents",
    "events",
    "Highlights",
    "highlights",
    "重点事件",
    "keyEvents",
  ];
  return aiKeys.some((key) => Object.prototype.hasOwnProperty.call(obj, key));
}

function locateAiPayload(data) {
  if (!data || typeof data !== "object") {
    return data;
  }
  const queue = [data];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") {
      continue;
    }
    if (hasAiMarkers(current)) {
      return current;
    }
    if (Array.isArray(current)) {
      current.forEach((item) => {
        if (item && typeof item === "object") {
          queue.push(item);
        }
      });
    } else {
      Object.values(current).forEach((value) => {
        if (value && typeof value === "object") {
          queue.push(value);
        }
      });
    }
  }
  return data;
}

function normalizeAiField(raw) {
  if (raw === null || raw === undefined) {
    return null;
  }
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = trySafeJSONParse(trimmed);
    if (parsed && typeof parsed === "object") {
      return parsed;
    }
    return trimmed;
  }
  return raw;
}

function buildFallbackRaw(summaryRaw, detailRaw, legacyRaw, summaryNormalized, detailNormalized, legacyNormalized) {
  const preferStrings = [legacyRaw, detailRaw, summaryRaw].map((value) => {
    if (typeof value === "string") {
      const trimmed = value.trim();
      return trimmed || null;
    }
    return null;
  });
  const firstString = preferStrings.find((value) => value);
  if (firstString) {
    return firstString;
  }
  const candidates = [legacyNormalized, detailNormalized, summaryNormalized];
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    if (typeof candidate === "string") {
      const trimmed = candidate.trim();
      if (trimmed) {
        return trimmed;
      }
      continue;
    }
    try {
      return JSON.stringify(candidate, null, 2);
    } catch (error) {
      // continue
    }
  }
  return null;
}

function extractSummaryText(payload) {
  if (!payload) {
    return "";
  }
  if (typeof payload === "string") {
    return payload.trim();
  }
  if (typeof payload !== "object") {
    return "";
  }
  const summaryKeys = [
    "当日总结",
    "今日总结",
    "日度总结",
    "今日综述",
    "日度概览",
    "总结",
    "總結",
    "总结陈词",
    "summary",
    "overallSummary",
    "analysisSummary",
    "AnalysisSummary",
    "分析摘要",
    "市场整体影响",
    "OverallMarketImpact",
    "marketOverallImpact",
    "市场观点",
    "overview",
  ];
  const direct = getFirstMatchingProperty(payload, summaryKeys);
  if (typeof direct === "string") {
    const trimmed = direct.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  const segments = [];
  Object.entries(payload).forEach(([key, value]) => {
    if (value == null) {
      return;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) {
        segments.push(`${key}: ${trimmed}`);
      }
    } else if (Array.isArray(value)) {
      const list = value.map((item) => cleanText(item)).filter(Boolean);
      if (list.length) {
        segments.push(`${key}: ${list.join("、")}`);
      }
    }
  });
  return segments.join(" ｜ ");
}

function extractAiEvents(payload) {
  if (!payload || typeof payload !== "object") {
    return [];
  }
  const candidate = getFirstMatchingProperty(payload, [
    "ImpactAnalysis",
    "影响事项",
    "影響事項",
    "影响分析",
    "impactAnalysis",
    "impact_items",
    "impactEvents",
    "events",
    "Highlights",
    "highlights",
    "重点事件",
    "keyEvents",
  ]);
  if (Array.isArray(candidate)) {
    return candidate;
  }
  if (candidate && typeof candidate === "object") {
    if (Array.isArray(candidate.items)) {
      return candidate.items;
    }
    return Object.values(candidate);
  }
  if (Array.isArray(payload)) {
    return payload;
  }
  return [];
}

function normalizeAiEvent(event) {
  if (!event || typeof event !== "object") {
    return null;
  }
  const title = cleanText(
    getFirstMatchingProperty(event, [
      "EventTitle",
      "事件标题",
      "事件名称",
      "事件名稱",
      "title",
      "name",
      "headline",
      "event",
    ])
  );
  const summary = cleanText(
    getFirstMatchingProperty(event, [
      "NewsSummary",
      "新闻摘要",
      "内容摘要",
      "简要摘要",
      "摘要",
      "summary",
      "description",
      "概述",
    ])
  );
  const direction = cleanText(
    getFirstMatchingProperty(event, [
      "ImpactNature",
      "影响性质",
      "影响方向",
      "方向",
      "direction",
      "impactDirection",
    ])
  );
  const intensity = parseIntensity(
    getFirstMatchingProperty(event, [
      "ImpactMagnitude",
      "影响程度",
      "影响强度",
      "强度",
      "impactScore",
      "score",
      "impactIntensity",
    ])
  );
  const scope = cleanText(
    getFirstMatchingProperty(event, [
      "ImpactScope",
      "影响范围",
      "范围",
      "scope",
      "impactScope",
    ])
  );
  const details = cleanText(
    getFirstMatchingProperty(event, [
      "Rationale",
      "影响理由",
      "影响逻辑",
      "逻辑",
      "影响详情",
      "详情",
      "details",
      "impactDetails",
      "reason",
    ])
  );
  const strategy = cleanText(
    getFirstMatchingProperty(event, [
      "InvestmentRecommendation",
      "投资策略",
      "策略",
      "strategy",
      "investmentStrategy",
      "investmentAdvice",
      "策略建议",
      "操作建议",
    ])
  );
  const duration = cleanText(
    getFirstMatchingProperty(event, [
      "Duration",
      "持续时间",
      "持续期",
      "影响持续期",
      "影响期限",
      "duration",
      "timeframe",
    ])
  );
  const targets = toList(
    getFirstMatchingProperty(event, [
      "AffectedSectors",
      "影响板块",
      "受益板块",
      "相关板块",
      "受益行业",
      "影响标的",
      "标的",
      "targets",
      "impactTargets",
      "securities",
    ])
  );
  const tags = toList(
    getFirstMatchingProperty(event, [
      "SectorTags",
      "板块标签",
      "主题标签",
      "事件标签",
      "标签",
      "标签列表",
      "tags",
      "labels",
      "keywords",
    ])
  );

  if (!title && !summary && !details) {
    return null;
  }

  return {
    title,
    summary,
    direction,
    intensity,
    scope,
    details,
    strategy,
    duration,
    targets,
    tags,
  };
}

function trySafeJSONParse(text) {
  try {
    return JSON.parse(text);
  } catch (error) {
    return null;
  }
}

function parseLooseAiText(text) {
  if (!text) {
    return { summary: "", events: [] };
  }

  const normalizedText = String(text);
  const summaryMatch = normalizedText.match(
    /(当日总结|今日总结|日度总结|今日综述|总结|综述)[：:]\s*([\s\S]*?)(?=(影响分析|影响事项|重点事件|AI分析|AI 摘要|$))/
  );
  const summary = summaryMatch ? summaryMatch[2].trim() : "";

  const eventMatches = normalizedText.match(/\{[^{}]*\}/g) || [];
  const events = eventMatches
    .map((chunk) => chunk.trim())
    .map((chunk) => {
      const sanitized = chunk.replace(/[\u201c\u201d]/g, '"').replace(/[\u2018\u2019]/g, "'");
      const parsed = trySafeJSONParse(sanitized);
      return parsed && typeof parsed === "object" ? parsed : null;
    })
    .filter(Boolean);

  return { summary, events };
}

function parseAiExtract(raw) {
  if (raw === null || raw === undefined) {
    return null;
  }
  let text = "";
  if (typeof raw === "string") {
    text = raw.trim();
  } else if (typeof raw === "object") {
    try {
      text = JSON.stringify(raw);
    } catch (error) {
      text = "";
    }
  } else {
    text = String(raw).trim();
  }

  if (!text) {
    return null;
  }

  function buildResult(summaryInput, eventsInput) {
    const normalizedEvents = (eventsInput || [])
      .map((item) => normalizeAiEvent(item))
      .filter(Boolean)
      .sort((a, b) => {
        const scoreA = Number.isFinite(a.intensity) ? a.intensity : -1;
        const scoreB = Number.isFinite(b.intensity) ? b.intensity : -1;
        return scoreB - scoreA;
      });

    let summaryTextValue = "";
    if (typeof summaryInput === "string") {
      summaryTextValue = summaryInput.trim();
    } else if (summaryInput) {
      summaryTextValue = extractSummaryText(summaryInput);
    }

    if (!summaryTextValue && normalizedEvents.length) {
      summaryTextValue = normalizedEvents
        .map((event) => event.summary || event.title)
        .filter(Boolean)
        .slice(0, 2)
        .join("；");
    }

    return {
      summaryText: summaryTextValue,
      events: normalizedEvents,
      rawText: text,
      structured: Boolean(summaryTextValue || normalizedEvents.length),
    };
  }

  let payload = null;
  if (typeof raw === "object" && raw !== null) {
    payload = raw;
  } else {
    payload = trySafeJSONParse(text);
    if (!payload) {
      const start = text.indexOf("{");
      const end = text.lastIndexOf("}");
      if (start !== -1 && end !== -1 && end > start) {
        payload = trySafeJSONParse(text.slice(start, end + 1));
      }
    }
  }

  if (payload && (typeof payload === "object" || Array.isArray(payload))) {
    const aiPayload = locateAiPayload(payload);
    const summarySource =
      getFirstMatchingProperty(aiPayload, [
        "当日总结",
        "今日总结",
        "日度总结",
        "dailySummary",
        "daySummary",
        "综合结论",
        "綜合結論",
        "analysis",
        "analysisResult",
        "overall",
        "overallAnalysis",
        "总结",
        "summary",
      ]) ?? aiPayload;
    const eventsRaw = extractAiEvents(aiPayload);
    const structured = buildResult(summarySource, eventsRaw);
    if (structured.structured) {
      return structured;
    }
  }

  const loose = parseLooseAiText(text);
  if (loose.summary || loose.events.length) {
    const structured = buildResult(loose.summary, loose.events);
    if (structured.structured) {
      return structured;
    }
  }

  return {
    summaryText: "",
    events: [],
    rawText: text,
    structured: false,
  };
}

function createAiEventItem(event, dict) {
  const item = document.createElement("li");
  item.className = "news-card__ai-item";

  const header = document.createElement("div");
  header.className = "news-card__ai-item-header";

  const title = document.createElement("div");
  title.className = "news-card__ai-item-title";
  title.textContent = event.title || event.summary || dict.aiImpactDetails;
  header.appendChild(title);

  if (event.direction) {
    const directionChip = document.createElement("span");
    directionChip.className = "news-card__ai-chip";
    directionChip.textContent = `${dict.aiImpactDirection}: ${event.direction}`;
    header.appendChild(directionChip);
  }

  if (Number.isFinite(event.intensity)) {
    const intensityChip = document.createElement("span");
    intensityChip.className = "news-card__ai-chip";
    intensityChip.textContent = `${dict.aiImpactIntensity}: ${Math.round(
      event.intensity
    )}`;
    header.appendChild(intensityChip);
  }

  item.appendChild(header);

  if (event.summary) {
    const summary = document.createElement("p");
    summary.className = "news-card__ai-item-summary";
    summary.textContent = event.summary;
    item.appendChild(summary);
  } else if (event.details) {
    const summary = document.createElement("p");
    summary.className = "news-card__ai-item-summary";
    summary.textContent = event.details;
    item.appendChild(summary);
  }

  const metaEntries = [
    event.scope ? { label: dict.aiImpactScope, value: event.scope } : null,
    event.targets.length
      ? { label: dict.aiImpactTargets, value: event.targets.join("、") }
      : null,
    event.duration ? { label: dict.aiImpactDuration, value: event.duration } : null,
    event.details ? { label: dict.aiImpactDetails, value: event.details } : null,
    event.strategy ? { label: dict.aiImpactStrategy, value: event.strategy } : null,
  ].filter(Boolean);

  if (metaEntries.length) {
    const metaList = document.createElement("ul");
    metaList.className = "news-card__ai-item-meta";
    metaEntries.forEach((meta) => {
      const metaItem = document.createElement("li");
      const label = document.createElement("strong");
      label.textContent = meta.label;
      metaItem.appendChild(label);
      metaItem.appendChild(document.createTextNode(meta.value));
      metaList.appendChild(metaItem);
    });
    item.appendChild(metaList);
  }

  if (event.tags.length) {
    const tagsList = document.createElement("ul");
    tagsList.className = "news-card__ai-item-tags";
    event.tags.forEach((tag) => {
      const tagItem = document.createElement("li");
      tagItem.className = "news-card__ai-tag";
      tagItem.textContent = tag;
      tagsList.appendChild(tagItem);
    });
    item.appendChild(tagsList);
  }

  return item;
}

function buildAiSection(summaryRaw, detailRaw, legacyRaw, dict) {
  const combinedPayload = {};
  let hasStructuredData = false;
  if (summaryRaw) {
    combinedPayload.ComprehensiveAssessment = summaryRaw;
    hasStructuredData = true;
  }
  if (detailRaw) {
    combinedPayload.ImpactAnalysis = detailRaw;
    hasStructuredData = true;
  }

  let analysis = null;
  if (hasStructuredData) {
    analysis = parseAiExtract(combinedPayload);
  }
  if (!analysis && legacyRaw) {
    analysis = parseAiExtract(legacyRaw);
  }
  if (!analysis) {
    return null;
  }
  const section = document.createElement("section");
  section.className = "news-card__ai";

  const heading = document.createElement("h3");
  heading.className = "news-card__ai-heading";
  heading.textContent = dict.aiInsightsTitle;
  section.appendChild(heading);

  if (analysis.summaryText) {
    const summaryHeading = document.createElement("h4");
    summaryHeading.className = "news-card__ai-subheading";
    summaryHeading.textContent = dict.aiSummaryHeading;
    section.appendChild(summaryHeading);

    const summary = document.createElement("p");
    summary.className = "news-card__ai-summary";
    summary.textContent = analysis.summaryText;
    section.appendChild(summary);
  }

  if (analysis.events.length) {
    const eventsHeading = document.createElement("h4");
    eventsHeading.className = "news-card__ai-subheading";
    eventsHeading.textContent = dict.aiEventsHeading;
    section.appendChild(eventsHeading);

    const list = document.createElement("ol");
    list.className = "news-card__ai-list";
    analysis.events.forEach((event) => {
      list.appendChild(createAiEventItem(event, dict));
    });
    section.appendChild(list);
  }

  if (!analysis.structured && analysis.rawText) {
    const rawBlock = document.createElement("pre");
    rawBlock.className = "news-card__ai-raw";
    rawBlock.textContent = analysis.rawText;
    section.appendChild(rawBlock);
  }

  return section;
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

    const summaryText =
      cleanText(entry.summary) ||
      cleanText(entry.title) ||
      cleanText(entry.content) ||
      dict.emptyState;
    const summary = document.createElement("p");
    summary.className = "news-card__summary";
    summary.textContent = summaryText;
    card.appendChild(summary);

    const meta = document.createElement("div");
    meta.className = "news-card__meta";
    meta.textContent = `${dict.publishedAt}: ${formatDate(entry.published_at)}`;
    card.appendChild(meta);

    if (entry.url) {
      const link = document.createElement("a");
      link.className = "news-card__link";
      link.href = entry.url;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = dict.readMore;
      if (entry.title) {
        link.setAttribute("aria-label", `${dict.readMore}: ${entry.title}`);
      }
      card.appendChild(link);
    }

    if (entry.title && entry.title.trim() && entry.title.trim() !== summaryText) {
      const titleWrapper = document.createElement("div");
      titleWrapper.className = "news-card__title";
      if (entry.url) {
        const anchor = document.createElement("a");
        anchor.href = entry.url;
        anchor.target = "_blank";
        anchor.rel = "noopener noreferrer";
        anchor.textContent = entry.title;
        titleWrapper.appendChild(anchor);
      } else {
        titleWrapper.textContent = entry.title;
      }
      card.appendChild(titleWrapper);
    }

    const aiSection = buildAiSection(
      entry.ai_extract_summary ?? entry.aiExtractSummary ?? null,
      entry.ai_extract_detail ?? entry.aiExtractDetail ?? null,
      entry.ai_extract ?? entry.aiExtract ?? null,
      dict
    );
    if (aiSection) {
      card.appendChild(aiSection);
    } else if (entry.ai_raw_fallback) {
      const fallbackSection = document.createElement("section");
      fallbackSection.className = "news-card__ai";

      const heading = document.createElement("h3");
      heading.className = "news-card__ai-heading";
      heading.textContent = dict.aiInsightsTitle || "AI Insights";
      fallbackSection.appendChild(heading);

      const rawBlock = document.createElement("pre");
      rawBlock.className = "news-card__ai-raw";
      rawBlock.textContent = entry.ai_raw_fallback;
      fallbackSection.appendChild(rawBlock);

      card.appendChild(fallbackSection);
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
      ? data.map((item) => {
          const aiSummaryRaw =
            item.aiExtractSummary !== undefined && item.aiExtractSummary !== null
              ? item.aiExtractSummary
              : item.ai_extract_summary ?? null;
          const aiDetailRaw =
            item.aiExtractDetail !== undefined && item.aiExtractDetail !== null
              ? item.aiExtractDetail
              : item.ai_extract_detail ?? null;
          const aiLegacyRaw =
            item.aiExtract !== undefined && item.aiExtract !== null ? item.aiExtract : item.ai_extract ?? null;

          const aiSummaryNormalized = normalizeAiField(aiSummaryRaw);
          const aiDetailNormalized = normalizeAiField(aiDetailRaw);
          const aiLegacyNormalized = normalizeAiField(aiLegacyRaw);
          const aiFallbackRaw = buildFallbackRaw(
            aiSummaryRaw,
            aiDetailRaw,
            aiLegacyRaw,
            aiSummaryNormalized,
            aiDetailNormalized,
            aiLegacyNormalized
          );

          return {
            title: item.title || "",
            summary: item.summary || "",
            content: item.content || "",
            ai_extract_summary: aiSummaryNormalized,
            ai_extract_detail: aiDetailNormalized,
            ai_extract: aiLegacyNormalized,
            ai_raw_fallback: aiFallbackRaw,
            published_at: item.publishedAt || item.published_at,
            url: item.url || "",
          };
        })
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

applyTranslations();
loadEntries();
