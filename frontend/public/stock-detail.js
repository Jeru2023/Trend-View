﻿const translations = getTranslations("stockDetail");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const LANG_STORAGE_KEY = "trend-view-lang";
const FAVORITES_GROUP_NONE = "__ungrouped__";
const FAVORITES_GROUP_NEW_OPTION = "__new__";

let currentLang = document.documentElement.getAttribute("data-pref-lang") || getInitialLanguage();
let candlestickData = [];
let currentDetail = null;
let candlestickChartInstance = null;
let candlestickRenderTimeout = null;
let performanceData = {
  express: null,
  forecast: null,
};
const detailExtras = {
  individualFundFlow: [],
  bigDeals: [],
};

const SEARCH_RESULT_LIMIT = 8;
const SEARCH_DEBOUNCE_MS = 260;

let searchDebounceTimer = null;
let searchAbortController = null;
let searchRequestToken = 0;
let searchBlurTimeout = null;

const searchState = {
  keyword: "",
  results: [],
  activeIndex: -1,
  loading: false,
  error: false,
};

const INDIVIDUAL_FUND_FLOW_SYMBOLS = [
  { match: "即时", key: "symbolInstant", label: "即时数据", translationKey: "stockDetailSymbolInstant" },
  { match: "3日排行", key: "symbol3Day", label: "3日数据", translationKey: "stockDetailSymbol3Day" },
  { match: "5日排行", key: "symbol5Day", label: "5日数据", translationKey: "stockDetailSymbol5Day" },
  { match: "10日排行", key: "symbol10Day", label: "10日数据", translationKey: "stockDetailSymbol10Day" },
  { match: "20日排行", key: "symbol20Day", label: "20日数据", translationKey: "stockDetailSymbol20Day" },
];

const INDIVIDUAL_FUND_FLOW_METRICS = {
  instant: [
    {
      valueKeys: ["netAmount", "net_amount"],
      labelKey: "individualFundFlowNetAmount",
      fallback: "Net Amount",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
      signed: true,
    },
    {
      valueKeys: ["inflow"],
      labelKey: "individualFundFlowInflow",
      fallback: "Inflow",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
    },
    {
      valueKeys: ["outflow"],
      labelKey: "individualFundFlowOutflow",
      fallback: "Outflow",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
    },
    {
      valueKeys: ["netInflow", "net_inflow"],
      labelKey: "individualFundFlowNetInflow",
      fallback: "Net Inflow",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
      signed: true,
    },
    {
      valueKeys: [
        "stageChangePercent",
        "stage_change_percent",
        "priceChangePercent",
        "price_change_percent",
      ],
      labelKey: "individualFundFlowStageChange",
      fallback: "Change (%)",
      formatter: (value) => formatPercentFlexible(value),
      signed: true,
    },
    {
      valueKeys: ["turnoverRate", "turnover_rate"],
      labelKey: "individualFundFlowTurnover",
      fallback: "Turnover (%)",
      formatter: (value) => formatPercentFlexible(value),
    },
    {
      valueKeys: ["continuousTurnoverRate", "continuous_turnover_rate"],
      labelKey: "individualFundFlowContinuousTurnover",
      fallback: "Continuous Turnover (%)",
      formatter: (value) => formatPercentFlexible(value),
    },
    {
      valueKeys: ["latestPrice", "latest_price"],
      labelKey: "individualFundFlowLatestPrice",
      fallback: "Last Price",
      formatter: (value) =>
        formatNumber(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    },
  ],
  ranked: [
    {
      valueKeys: ["netInflow", "net_inflow"],
      labelKey: "individualFundFlowNetInflow",
      fallback: "Net Inflow",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
      signed: true,
    },
    {
      valueKeys: ["netAmount", "net_amount"],
      labelKey: "individualFundFlowNetAmount",
      fallback: "Net Amount",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
      signed: true,
    },
    {
      valueKeys: ["inflow"],
      labelKey: "individualFundFlowInflow",
      fallback: "Inflow",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
    },
    {
      valueKeys: ["outflow"],
      labelKey: "individualFundFlowOutflow",
      fallback: "Outflow",
      formatter: (value) => formatCurrency(value, { maximumFractionDigits: 0 }),
    },
    {
      valueKeys: [
        "stageChangePercent",
        "stage_change_percent",
        "priceChangePercent",
        "price_change_percent",
      ],
      labelKey: "individualFundFlowStageChange",
      fallback: "Change (%)",
      formatter: (value) => formatPercentFlexible(value),
      signed: true,
    },
    {
      valueKeys: ["turnoverRate", "turnover_rate"],
      labelKey: "individualFundFlowTurnover",
      fallback: "Turnover (%)",
      formatter: (value) => formatPercentFlexible(value),
    },
    {
      valueKeys: ["continuousTurnoverRate", "continuous_turnover_rate"],
      labelKey: "individualFundFlowContinuousTurnover",
      fallback: "Continuous Turnover (%)",
      formatter: (value) => formatPercentFlexible(value),
    },
    {
      valueKeys: ["latestPrice", "latest_price"],
      labelKey: "individualFundFlowLatestPrice",
      fallback: "Last Price",
      formatter: (value) =>
        formatNumber(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    },
  ],
};

const elements = {
  status: document.getElementById("detail-status"),
  grid: document.getElementById("detail-grid"),
  hero: document.getElementById("detail-hero"),
  title: document.getElementById("detail-title"),
  subtitle: document.getElementById("detail-subtitle"),
  heroPrice: document.getElementById("hero-price"),
  heroChange: document.getElementById("hero-change"),
  heroMeta: document.getElementById("hero-meta"),
  heroUpdated: document.getElementById("hero-updated"),
  heroMarketCap: document.getElementById("hero-market-cap"),
  heroVolume: document.getElementById("hero-volume"),
  heroPe: document.getElementById("hero-pe"),
  heroTurnover: document.getElementById("hero-turnover"),
  financialList: document.getElementById("financial-list"),
  statsList: document.getElementById("stats-list"),
  fundamentalsList: document.getElementById("fundamentals-list"),
  businessSection: document.getElementById("business-composition-section"),
  mainBusinessCard: document.getElementById("main-business-card"),
  mainBusinessList: document.getElementById("main-business-list"),
  mainBusinessEmpty: document.getElementById("main-business-empty"),
  mainCompositionCard: document.getElementById("main-composition-card"),
  mainCompositionGroups: document.getElementById("main-composition-groups"),
  mainCompositionDate: document.getElementById("main-composition-report-date"),
  mainCompositionEmpty: document.getElementById("main-composition-empty"),
  langButtons: document.querySelectorAll(".lang-btn"),
  candlestickContainer: document.getElementById("candlestick-chart"),
  candlestickEmpty: document.getElementById("candlestick-empty"),
  favoriteToggle: document.getElementById("favorite-toggle"),
  xueqiuLink: document.getElementById("detail-link-xueqiu"),
  eastmoneyLink: document.getElementById("detail-link-eastmoney"),
  performanceCard: document.getElementById("performance-card"),
  performanceExpressTile: document.getElementById("performance-express-tile"),
  performanceForecastTile: document.getElementById("performance-forecast-tile"),
  individualFundFlowCard: document.getElementById("individual-fund-flow-card"),
  individualFundFlowList: document.getElementById("individual-fund-flow-list"),
  bigDealCard: document.getElementById("big-deal-card"),
  bigDealList: document.getElementById("big-deal-list"),
  searchWrapper: document.getElementById("detail-search"),
  searchInput: document.getElementById("detail-search-input"),
  searchResults: document.getElementById("detail-search-results"),
};

const favoriteState = {
  code: null,
  isFavorite: false,
  group: null,
  busy: false,
};

const toastState = {
  container: null,
};

const favoriteGroupsCache = {
  items: [],
  loaded: false,
  loading: null,
};

function normalizeCode(value) {
  const raw = (value || "").trim().toUpperCase();
  if (!raw) {
    return "";
  }

  if (raw.includes(".")) {
    const [symbolPart, suffixPart] = raw.split(".", 2);
    const normalizedSymbol = /^\d+$/.test(symbolPart) ? symbolPart.padStart(6, "0") : symbolPart;
    return suffixPart ? `${normalizedSymbol}.${suffixPart}` : normalizedSymbol;
  }

  const exchangeMatch = raw.match(/^(SH|SZ|BJ)(\d{5,6})$/);
  if (exchangeMatch) {
    const [, prefix, digits] = exchangeMatch;
    return `${digits.padStart(6, "0")}.${prefix}`;
  }

  const plainDigits = raw.match(/^(\d{6})$/);
  if (plainDigits) {
    const digits = plainDigits[1];
    const first = digits[0];
    let suffix = "";
    if (digits.startsWith("43") || digits.startsWith("83") || digits.startsWith("87") || first === "4" || first === "8") {
      suffix = "BJ";
    } else if (first === "6" || first === "9" || first === "5") {
      suffix = "SH";
    } else if (first === "0" || first === "2" || first === "3") {
      suffix = "SZ";
    }
    return suffix ? `${digits}.${suffix}` : digits;
  }

  return raw;
}

function deriveCodeParts(value) {
  const normalized = normalizeCode(value);
  if (!normalized) {
    return { detailCode: "", displayCode: "" };
  }
  if (normalized.includes(".")) {
    const [symbolPart] = normalized.split(".", 2);
    return { detailCode: normalized, displayCode: symbolPart };
  }
  const digitsOnly = normalized.replace(/\D/g, "");
  if (digitsOnly) {
    return { detailCode: normalized, displayCode: digitsOnly.padStart(6, "0") };
  }
  return { detailCode: normalized, displayCode: normalized };
}

function buildExternalLinks(value) {
  const normalized = normalizeCode(value);
  const match = normalized.match(/^(\d{6})\.(SH|SZ|BJ)$/);
  if (!match) {
    return { xueqiu: "", eastmoney: "" };
  }
  const [, digits, suffix] = match;
  const upperSuffix = suffix.toUpperCase();
  const lowerSuffix = suffix.toLowerCase();
  return {
    xueqiu: `https://xueqiu.com/S/${upperSuffix}${digits}`,
    eastmoney: `https://quote.eastmoney.com/${lowerSuffix}${digits}.html`,
  };
}

function toggleExternalLink(element, url) {
  if (!element) {
    return;
  }
  if (url) {
    element.href = url;
    element.classList.remove("hidden");
  } else {
    element.removeAttribute("href");
    element.classList.add("hidden");
  }
}

function updateExternalLinks(code) {
  const links = buildExternalLinks(code);
  toggleExternalLink(elements.xueqiuLink, links.xueqiu);
  toggleExternalLink(elements.eastmoneyLink, links.eastmoney);
}

function updateMainBusinessCard(profile) {
  const card = elements.mainBusinessCard;
  const list = elements.mainBusinessList;
  const emptyState = elements.mainBusinessEmpty;
  if (!card || !list || !emptyState) {
    return;
  }

  if (!profile) {
    card.classList.add("hidden");
    list.innerHTML = "";
    emptyState.classList.add("hidden");
    updateBusinessSectionVisibility();
    return;
  }

  const dict = translations[currentLang];
  const normalize = (value) => {
    if (value === null || value === undefined) {
      return "";
    }
    const text = String(value).trim();
    if (!text || text === "--" || text === "-") {
      return "";
    }
    return text;
  };

  const items = [
    {
      label: dict.mainBusinessLabelMain || "Main Business",
      value: normalize(profile.mainBusiness),
    },
    {
      label: dict.mainBusinessLabelProductType || "Product Categories",
      value: normalize(profile.productType),
    },
    {
      label: dict.mainBusinessLabelProductName || "Product Names",
      value: normalize(profile.productName),
    },
    {
      label: dict.mainBusinessLabelScope || "Business Scope",
      value: normalize(profile.businessScope),
    },
  ].filter((item) => item.value);

  if (!items.length) {
    list.innerHTML = "";
    emptyState.classList.add("hidden");
    card.classList.add("hidden");
    updateBusinessSectionVisibility();
    return;
  }

  list.innerHTML = items
    .map((item, index) => {
      const classes = ["business-info__item"];
      if (index === items.length - 1) {
        classes.push("business-info__item--full");
      }
      return `
        <div class="${classes.join(" ")}">
          <span class="business-info__label">${item.label}</span>
          <p class="business-info__value">${item.value}</p>
        </div>
      `;
    })
    .join("");

  emptyState.classList.add("hidden");
  card.classList.remove("hidden");
  updateBusinessSectionVisibility();
}

function updateMainCompositionCard(composition) {
  const card = elements.mainCompositionCard;
  const groupsContainer = elements.mainCompositionGroups;
  const emptyState = elements.mainCompositionEmpty;
  const dateElement = elements.mainCompositionDate;
  if (!card || !groupsContainer || !emptyState) {
    return;
  }

  if (!composition || !Array.isArray(composition.groups) || composition.groups.length === 0) {
    card.classList.add("hidden");
    groupsContainer.innerHTML = "";
    emptyState.classList.add("hidden");
    updateBusinessSectionVisibility();
    return;
  }

  const dict = translations[currentLang];
  const latestDate = composition.latestReportDate ? formatDate(composition.latestReportDate) : "";
  if (dateElement) {
    if (latestDate) {
      const template = dict.mainCompositionReportDate || "Latest report: {date}";
      dateElement.textContent = template.replace("{date}", latestDate);
      dateElement.classList.remove("hidden");
    } else {
      dateElement.textContent = "";
      dateElement.classList.add("hidden");
    }
  }

  groupsContainer.innerHTML = "";

  const toRatio = (value) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return null;
    }
    if (numeric > 1 || numeric < -1) {
      return numeric / 100;
    }
    return numeric;
  };

  const groups = Array.isArray(composition.groups) ? composition.groups : [];
  let renderedGroups = 0;

  for (const group of groups) {
    const entries = Array.isArray(group.entries) ? group.entries.filter((item) => item && item.composition) : [];
    if (!entries.length) {
      continue;
    }

    entries.sort((a, b) => {
      const ratioA = Math.abs(toRatio(a.revenueRatio) ?? 0);
      const ratioB = Math.abs(toRatio(b.revenueRatio) ?? 0);
      return ratioB - ratioA;
    });

    renderedGroups += 1;
    const groupWrapper = document.createElement("div");
    groupWrapper.className = "composition-group";

    const title = document.createElement("h3");
    title.className = "composition-group__title";
    title.textContent = group.categoryType || dict.mainCompositionCategoryUnknown || "Uncategorized";
    groupWrapper.appendChild(title);

    const entriesWrapper = document.createElement("div");
    entriesWrapper.className = "composition-group__entries";
    const categoryLabel = group.categoryType || "";
    entriesWrapper.dataset.layout = categoryLabel.includes("地区") ? "region" : "product";

    entries.forEach((entry) => {
      const entryNode = document.createElement("div");
      entryNode.className = "composition-entry";

      const header = document.createElement("div");
      header.className = "composition-entry__header";
      const name = document.createElement("span");
      name.className = "composition-entry__name";
      name.textContent = entry.composition || dict.mainCompositionValueUnknown || "--";
      const ratioSpan = document.createElement("span");
      ratioSpan.className = "composition-entry__ratio";
      ratioSpan.textContent = formatPercentFlexible(entry.revenueRatio);
      header.appendChild(name);
      header.appendChild(ratioSpan);
      entryNode.appendChild(header);

      const metrics = document.createElement("div");
      metrics.className = "composition-entry__metrics";
      if (entry.revenue !== null && entry.revenue !== undefined) {
        metrics.appendChild(
          document.createTextNode(`${dict.mainCompositionColumnRevenue || "Revenue"}: ${formatCompactNumber(entry.revenue)}`)
        );
      }
      if (entry.profitRatio !== null && entry.profitRatio !== undefined) {
        metrics.appendChild(
          document.createTextNode(
            `${dict.mainCompositionColumnProfitRatio || "Profit %"}: ${formatPercentFlexible(entry.profitRatio)}`
          )
        );
      }
      if (entry.grossMargin !== null && entry.grossMargin !== undefined) {
        metrics.appendChild(
          document.createTextNode(
            `${dict.mainCompositionColumnGrossMargin || "Gross Margin"}: ${formatPercentFlexible(entry.grossMargin)}`
          )
        );
      }
      if (metrics.childNodes.length) {
        entryNode.appendChild(metrics);
      }

      const progress = document.createElement("div");
      progress.className = "composition-entry__bar";
      const fill = document.createElement("span");
      const ratioValue = toRatio(entry.revenueRatio);
      const clamped = ratioValue === null ? 0 : Math.max(0, Math.min(1, ratioValue));
      fill.style.width = `${(clamped * 100).toFixed(1)}%`;
      progress.appendChild(fill);
      entryNode.appendChild(progress);

      entriesWrapper.appendChild(entryNode);
    });

    groupWrapper.appendChild(entriesWrapper);
    groupsContainer.appendChild(groupWrapper);
  }

  if (renderedGroups === 0) {
    card.classList.add("hidden");
    emptyState.classList.add("hidden");
    updateBusinessSectionVisibility();
    return;
  }

  emptyState.classList.add("hidden");
  card.classList.remove("hidden");
  updateBusinessSectionVisibility();
}

function updateBusinessSectionVisibility() {
  const section = elements.businessSection;
  if (!section || !elements.mainBusinessCard || !elements.mainCompositionCard) {
    return;
  }
  const hasBusiness = !elements.mainBusinessCard.classList.contains("hidden");
  const hasComposition = !elements.mainCompositionCard.classList.contains("hidden");
  section.classList.toggle("hidden", !(hasBusiness || hasComposition));
}

function getRecordValue(record, ...keys) {
  if (!record) {
    return undefined;
  }
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(record, key)) {
      const value = record[key];
      if (value === null || value === undefined) {
        continue;
      }
      if (typeof value === "number" && Number.isNaN(value)) {
        continue;
      }
      if (typeof value === "string") {
        const normalized = value.trim();
        if (!normalized || normalized === "--" || normalized === "-") {
          continue;
        }
      }
      return value;
    }
    const fallback = record[key];
    if (fallback === null || fallback === undefined) {
      continue;
    }
    if (typeof fallback === "number" && Number.isNaN(fallback)) {
      continue;
    }
    if (typeof fallback === "string") {
      const normalizedFallback = fallback.trim();
      if (!normalizedFallback || normalizedFallback === "--" || normalizedFallback === "-") {
        continue;
      }
    }
    return fallback;
  }
  return undefined;
}

function toNumeric(value) {
  if (value === null || value === undefined) {
    return Number.NaN;
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.replace(/,/g, "").trim();
    if (!normalized || normalized === "--" || normalized === "-") {
      return Number.NaN;
    }
    const numeric = Number(normalized);
    return Number.isFinite(numeric) ? numeric : Number.NaN;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : Number.NaN;
}

function buildIndividualFundFlowMetrics(entry, variant, dict) {
  const configs = INDIVIDUAL_FUND_FLOW_METRICS[variant] || [];
  const metrics = [];
  for (const config of configs) {
    const raw = getRecordValue(entry, ...(config.valueKeys || []));
    const numeric = toNumeric(raw);
    if (!Number.isFinite(numeric)) {
      continue;
    }
    const label = dict[config.labelKey] || config.fallback;
    const formatted = config.formatter ? config.formatter(numeric) : formatNumber(numeric);
    const value = config.signed ? wrapSignedValue(numeric, formatted) : escapeHTML(formatted);
    metrics.push({ label, value });
  }
  return metrics;
}

function normalizeFavoriteGroupValue(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  return value;
}

function ensureToastContainer() {
  if (toastState.container && document.body.contains(toastState.container)) {
    return toastState.container;
  }
  const container = document.createElement("div");
  container.className = "toast-container";
  document.body.appendChild(container);
  toastState.container = container;
  return container;
}

function showToast(message, type = "success", duration = 3200) {
  const container = ensureToastContainer();
  const toast = document.createElement("div");
  toast.className = `toast toast--${type}`;

  const text = document.createElement("span");
  text.className = "toast__message";
  text.textContent = message;
  toast.appendChild(text);
  container.appendChild(toast);

  const removeToast = () => {
    toast.classList.add("toast--closing");
    toast.addEventListener(
      "transitionend",
      () => {
        toast.remove();
        if (!container.hasChildNodes()) {
          container.remove();
          toastState.container = null;
        }
      },
      { once: true }
    );
  };

  const timeoutId = setTimeout(removeToast, duration);
  toast.addEventListener("click", () => {
    clearTimeout(timeoutId);
    removeToast();
  });
}

function normalizeFavoriteGroupForRequest(value) {
  const normalized = normalizeFavoriteGroupValue(value);
  if (!normalized) {
    return null;
  }
  if (normalized === FAVORITES_GROUP_NONE) {
    return FAVORITES_GROUP_NONE;
  }
  return normalized;
}

function formatFavoriteGroupLabel(value) {
  const dict = translations[currentLang] || {};
  const normalized = normalizeFavoriteGroupValue(value);
  if (!normalized || normalized === FAVORITES_GROUP_NONE) {
    return dict.favoriteGroupNone || "Ungrouped";
  }
  return normalized;
}

async function loadFavoriteGroups(force = false) {
  if (!force && favoriteGroupsCache.loaded) {
    return favoriteGroupsCache.items;
  }
  if (favoriteGroupsCache.loading) {
    return favoriteGroupsCache.loading;
  }
  favoriteGroupsCache.loading = (async () => {
    try {
      const response = await fetch(`${API_BASE}/favorites/groups`);
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const data = await response.json();
      const items = Array.isArray(data?.items) ? data.items : [];
      favoriteGroupsCache.items = items.map((entry) => {
        const normalizedName = normalizeFavoriteGroupValue(entry?.name ?? null);
        const value =
          normalizedName === null ? FAVORITES_GROUP_NONE : String(normalizedName);
        const total = Number(entry?.total ?? 0);
        return { name: normalizedName, value, total };
      });
      favoriteGroupsCache.loaded = true;
      return favoriteGroupsCache.items;
    } catch (error) {
      favoriteGroupsCache.loaded = false;
      favoriteGroupsCache.items = [];
      throw error;
    } finally {
      favoriteGroupsCache.loading = null;
    }
  })();
  return favoriteGroupsCache.loading;
}

async function openFavoriteGroupDialog(groups, initialGroup = FAVORITES_GROUP_NONE) {
  return new Promise((resolve) => {
    const dict = translations[currentLang] || {};
    const backdrop = document.createElement("div");
    backdrop.className = "favorite-dialog-backdrop";

    const dialog = document.createElement("div");
    dialog.className = "favorite-dialog";
    backdrop.appendChild(dialog);

    const title = document.createElement("h2");
    title.className = "favorite-dialog__title";
    title.textContent = dict.favoriteGroupDialogTitle || "Choose a group";
    dialog.appendChild(title);

    const body = document.createElement("div");
    body.className = "favorite-dialog__body";
    dialog.appendChild(body);

    const selectLabel = document.createElement("label");
    const selectId = "favorite-group-select";
    selectLabel.setAttribute("for", selectId);
    selectLabel.textContent = dict.favoriteGroupDialogExistingLabel || "Existing groups";
    body.appendChild(selectLabel);

    const select = document.createElement("select");
    select.className = "favorite-dialog__select";
    select.id = selectId;

    const ungroupedOption = document.createElement("option");
    ungroupedOption.value = FAVORITES_GROUP_NONE;
    ungroupedOption.textContent = dict.favoriteGroupNone || "Ungrouped";
    select.appendChild(ungroupedOption);

    const sortedGroups = (groups || [])
      .filter((entry) => entry.value !== FAVORITES_GROUP_NONE)
      .sort((a, b) => a.value.localeCompare(b.value));

    sortedGroups.forEach((entry) => {
      const option = document.createElement("option");
      option.value = entry.value;
      option.textContent = entry.value;
      select.appendChild(option);
    });

    if (
      initialGroup &&
      initialGroup !== FAVORITES_GROUP_NONE &&
      !sortedGroups.some((entry) => entry.value === initialGroup)
    ) {
      const fallbackOption = document.createElement("option");
      fallbackOption.value = initialGroup;
      fallbackOption.textContent = initialGroup;
      select.appendChild(fallbackOption);
    }

    const newOption = document.createElement("option");
    newOption.value = FAVORITES_GROUP_NEW_OPTION;
    newOption.textContent = dict.favoriteGroupDialogNewOption || "Create new group";
    select.appendChild(newOption);

    body.appendChild(select);

    const input = document.createElement("input");
    input.type = "text";
    input.className = "favorite-dialog__input";
    input.placeholder = dict.favoriteGroupDialogNewPlaceholder || "Enter new group name";
    input.autocomplete = "off";
    input.style.display = "none";
    body.appendChild(input);

    const note = document.createElement("div");
    note.className = "favorite-dialog__note";
    if (dict.favoriteGroupDialogNote) {
      note.textContent = dict.favoriteGroupDialogNote;
      body.appendChild(note);
    }

    const error = document.createElement("div");
    error.className = "favorite-dialog__error";
    error.style.display = "none";
    body.appendChild(error);

    const actions = document.createElement("div");
    actions.className = "favorite-dialog__actions";
    dialog.appendChild(actions);

    const cancelButton = document.createElement("button");
    cancelButton.type = "button";
    cancelButton.className = "secondary-btn";
    cancelButton.textContent = dict.favoriteGroupDialogCancel || "Cancel";
    actions.appendChild(cancelButton);

    const confirmButton = document.createElement("button");
    confirmButton.type = "button";
    confirmButton.className = "primary-btn";
    confirmButton.textContent = dict.favoriteGroupDialogConfirm || "Save";
    actions.appendChild(confirmButton);

    function toggleInputVisibility() {
      if (select.value === FAVORITES_GROUP_NEW_OPTION) {
        input.style.display = "block";
        input.focus();
      } else {
        input.style.display = "none";
        input.value = "";
      }
      error.style.display = "none";
      error.textContent = "";
    }

    function cleanup(result) {
      document.removeEventListener("keydown", onKeyDown);
      backdrop.remove();
      resolve(result);
    }

    function onKeyDown(event) {
      if (event.key === "Escape") {
        cleanup(undefined);
      }
      if (event.key === "Enter") {
        event.preventDefault();
        confirmButton.click();
      }
    }

    select.addEventListener("change", toggleInputVisibility);
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        confirmButton.click();
      }
    });

    cancelButton.addEventListener("click", () => cleanup(undefined));

    confirmButton.addEventListener("click", () => {
      if (select.value === FAVORITES_GROUP_NEW_OPTION) {
        const newGroupName = input.value.trim();
        if (!newGroupName) {
          error.textContent =
            dict.favoriteGroupDialogErrorRequired || "Please enter a group name.";
          error.style.display = "block";
          input.focus();
          return;
        }
        cleanup(newGroupName);
        return;
      }
      const resolvedGroup = select.value === FAVORITES_GROUP_NONE ? null : select.value;
      cleanup(resolvedGroup);
    });

    backdrop.addEventListener("mousedown", (event) => {
      if (event.target === backdrop) {
        cleanup(undefined);
      }
    });

    document.addEventListener("keydown", onKeyDown);

    document.body.appendChild(backdrop);
    select.value = initialGroup ?? FAVORITES_GROUP_NONE;
    requestAnimationFrame(() => {
      toggleInputVisibility();
      select.focus();
    });
  });
}

async function requestFavoriteGroupSelection() {
  try {
    const groups = await loadFavoriteGroups();
    const initialGroup = favoriteState.group ?? FAVORITES_GROUP_NONE;
    return await openFavoriteGroupDialog(groups, initialGroup);
  } catch (error) {
    console.error("Failed to load favorite groups:", error);
    const dict = translations[currentLang] || {};
    const fallback = window.prompt(
      dict.favoriteGroupDialogFallback || "Enter a group name (leave blank for ungrouped)",
      favoriteState.group || ""
    );
    if (fallback === null) {
      return undefined;
    }
    const trimmed = fallback.trim();
    return trimmed ? trimmed : null;
  }
}
function setFavoriteBusy(isBusy) {
  favoriteState.busy = Boolean(isBusy);
  if (!elements.favoriteToggle) {
    return;
  }
  const shouldDisable = favoriteState.busy || !favoriteState.code;
  elements.favoriteToggle.disabled = shouldDisable;
}

function updateFavoriteToggle(isFavorite = favoriteState.isFavorite) {
  favoriteState.isFavorite = Boolean(isFavorite);
  if (!elements.favoriteToggle) {
    return;
  }
  const dict = translations[currentLang] || {};
  const labelKey = favoriteState.isFavorite ? "favoriteToggleLabelRemove" : "favoriteToggleLabelAdd";
  const tooltipKey = favoriteState.isFavorite ? "favoriteToggleTooltipRemove" : "favoriteToggleTooltipAdd";
  elements.favoriteToggle.setAttribute("aria-pressed", String(favoriteState.isFavorite));
  elements.favoriteToggle.classList.toggle("favorite-toggle--active", favoriteState.isFavorite);
  elements.favoriteToggle.title = dict[tooltipKey] || "";
  elements.favoriteToggle.dataset.favoriteGroup = favoriteState.group ?? "";

  const srLabel = elements.favoriteToggle.querySelector(".sr-only");
  if (srLabel) {
    srLabel.dataset.i18n = labelKey;
    srLabel.textContent = dict[labelKey] || "";
  }
}

async function submitFavoriteState(shouldFavorite, { group = favoriteState.group } = {}) {
  if (!favoriteState.code) {
    return;
  }
  setFavoriteBusy(true);
  const requestOptions = {
    method: shouldFavorite ? "PUT" : "DELETE",
    headers: {},
  };
  if (shouldFavorite) {
    requestOptions.headers["Content-Type"] = "application/json";
    requestOptions.body = JSON.stringify({
      group: normalizeFavoriteGroupForRequest(group),
    });
  }

  try {
    const response = await fetch(
      `${API_BASE}/favorites/${encodeURIComponent(favoriteState.code)}`,
      requestOptions
    );
    if (!response.ok) {
      throw new Error(`Favorite toggle failed with status ${response.status}`);
    }
    const payload = await response.json();
    const nextIsFavorite = Boolean(payload?.isFavorite ?? shouldFavorite);
    const nextGroup = payload?.group ?? (shouldFavorite ? group ?? null : null);

    favoriteState.isFavorite = nextIsFavorite;
    favoriteState.group = normalizeFavoriteGroupValue(nextGroup);
    updateFavoriteToggle(nextIsFavorite);

    if (currentDetail) {
      currentDetail.isFavorite = nextIsFavorite;
      currentDetail.favoriteGroup = favoriteState.group;
      if (currentDetail.profile) {
        currentDetail.profile.isFavorite = nextIsFavorite;
        currentDetail.profile.favoriteGroup = favoriteState.group;
      }
    }

    const dict = translations[currentLang] || {};
    if (nextIsFavorite) {
      const messageTemplate = dict.favoriteToastAdded || "Added to watchlist ({group})";
      const message = messageTemplate.replace(
        "{group}",
        formatFavoriteGroupLabel(favoriteState.group)
      );
      showToast(message, "success");
    } else {
      const message = dict.favoriteToastRemoved || "Removed from watchlist";
      showToast(message, "info");
    }

    favoriteGroupsCache.loaded = false;
  } catch (error) {
    console.error("Failed to toggle favorite:", error);
    const dict = translations[currentLang] || {};
    showToast(
      dict.favoriteToggleError || "Unable to update watchlist. Please try again.",
      "error"
    );
  } finally {
    setFavoriteBusy(false);
  }
}

async function handleFavoriteToggle() {
  if (favoriteState.busy || !favoriteState.code) {
    return;
  }
  if (favoriteState.isFavorite) {
    await submitFavoriteState(false);
    return;
  }
  try {
    const selectedGroup = await requestFavoriteGroupSelection();
    if (selectedGroup === undefined) {
      return;
    }
    await submitFavoriteState(true, { group: selectedGroup });
  } catch (error) {
    console.error("Favorite toggle request failed:", error);
  }
}

function initFavoriteToggle() {
  if (!elements.favoriteToggle) {
    return;
  }
  elements.favoriteToggle.addEventListener("click", () => {
    handleFavoriteToggle();
  });
  setFavoriteBusy(true);
  updateFavoriteToggle(false);
}

function resetSearchState() {
  searchState.keyword = "";
  searchState.results = [];
  searchState.activeIndex = -1;
  searchState.loading = false;
  searchState.error = false;
}

function hideSearchResults() {
  if (elements.searchResults) {
    elements.searchResults.classList.add("hidden");
    elements.searchResults.innerHTML = "";
  }
  if (elements.searchInput) {
    elements.searchInput.setAttribute("aria-expanded", "false");
  }
}

function clearSearchInterface() {
  resetSearchState();
  hideSearchResults();
}

function ensureActiveOptionVisible() {
  if (!elements.searchResults) {
    return;
  }
  const index = searchState.activeIndex;
  if (index < 0) {
    return;
  }
  const activeOption = elements.searchResults.querySelector(
    `.detail-search__result[data-index="${index}"]`
  );
  if (activeOption && typeof activeOption.scrollIntoView === "function") {
    activeOption.scrollIntoView({ block: "nearest" });
  }
}

function applySearchActiveState() {
  if (!elements.searchResults) {
    return;
  }
  const options = elements.searchResults.querySelectorAll(".detail-search__result");
  options.forEach((option) => {
    const optionIndex = Number.parseInt(option.dataset.index || "-1", 10);
    const isActive = optionIndex === searchState.activeIndex;
    option.classList.toggle("detail-search__result--active", isActive);
    option.setAttribute("aria-selected", isActive ? "true" : "false");
  });
  ensureActiveOptionVisible();
}

function renderSearchResults() {
  if (!elements.searchResults || !elements.searchInput) {
    return;
  }
  const container = elements.searchResults;
  container.innerHTML = "";
  const keyword = searchState.keyword;
  if (!keyword) {
    hideSearchResults();
    return;
  }
  elements.searchInput.setAttribute("aria-expanded", "true");
  container.classList.remove("hidden");
  const dict = translations[currentLang] || {};
  if (searchState.loading) {
    const status = document.createElement("div");
    status.className = "detail-search__status";
    status.textContent = dict.searchLoading || "Searching...";
    container.appendChild(status);
    return;
  }
  if (searchState.error) {
    const status = document.createElement("div");
    status.className = "detail-search__status detail-search__status--error";
    status.textContent =
      dict.searchError || "Unable to fetch search results. Please try again later.";
    container.appendChild(status);
    return;
  }
  if (!searchState.results.length) {
    const status = document.createElement("div");
    status.className = "detail-search__status";
    const template = dict.searchNoResults || 'No matches for "{keyword}"';
    status.textContent = template.replace("{keyword}", keyword);
    container.appendChild(status);
    return;
  }
  const fragment = document.createDocumentFragment();
  searchState.results.forEach((item, index) => {
    const option = document.createElement("button");
    option.type = "button";
    option.className = "detail-search__result";
    option.dataset.index = String(index);
    option.setAttribute("role", "option");
    option.setAttribute("aria-selected", "false");
    option.addEventListener("mousedown", (event) => {
      event.preventDefault();
      if (item && item.code) {
        navigateToStock(item.code);
      }
    });
    option.addEventListener("mouseenter", () => {
      searchState.activeIndex = index;
      applySearchActiveState();
    });
    const primary = document.createElement("div");
    primary.className = "detail-search__result-primary";
    const codeSpan = document.createElement("span");
    codeSpan.className = "detail-search__result-code";
    codeSpan.textContent = (item && item.code) || "--";
    primary.appendChild(codeSpan);
    if (item && item.name) {
      const nameSpan = document.createElement("span");
      nameSpan.className = "detail-search__result-name";
      nameSpan.textContent = item.name;
      primary.appendChild(nameSpan);
    }
    option.appendChild(primary);
    const metaParts = [];
    if (item && item.industry) {
      metaParts.push(item.industry);
    }
    if (item && item.market) {
      metaParts.push(item.market);
    }
    if (item && item.exchange) {
      metaParts.push(item.exchange);
    }
    if (metaParts.length) {
      const meta = document.createElement("div");
      meta.className = "detail-search__result-meta";
      meta.textContent = metaParts.join(" · ");
      option.appendChild(meta);
    }
    fragment.appendChild(option);
  });
  container.appendChild(fragment);
  applySearchActiveState();
}

function scheduleSearch(keyword) {
  const value = typeof keyword === "string" ? keyword.trim() : "";
  if (!value) {
    if (searchDebounceTimer) {
      clearTimeout(searchDebounceTimer);
      searchDebounceTimer = null;
    }
    if (searchAbortController) {
      searchAbortController.abort();
      searchAbortController = null;
    }
    clearSearchInterface();
    return;
  }
  searchState.keyword = value;
  searchState.activeIndex = -1;
  searchState.error = false;
  searchState.loading = true;
  if (searchAbortController) {
    searchAbortController.abort();
    searchAbortController = null;
  }
  if (searchDebounceTimer) {
    clearTimeout(searchDebounceTimer);
  }
  renderSearchResults();
  searchDebounceTimer = window.setTimeout(() => {
    performSearch(value);
  }, SEARCH_DEBOUNCE_MS);
}

async function fetchLegacySearchResults(value, controller) {
  const params = new URLSearchParams();
  params.set("limit", String(SEARCH_RESULT_LIMIT));
  params.set("keyword", value);
  params.set("searchOnly", "true");
  params.set("pctChangeMin", "2");
  params.set("pctChangeMax", "5");
  params.set("volumeSpikeMin", "1.8");
  params.set("peMin", "0");
  params.set("roeMin", "3");
  params.set("netIncomeQoqMin", "0");
  params.set("netIncomeYoyMin", "0.1");
  const response = await fetch(`${API_BASE}/stocks?${params.toString()}`, {
    signal: controller.signal,
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

async function fetchSearchResults(value, controller) {
  const params = new URLSearchParams();
  params.set("limit", String(SEARCH_RESULT_LIMIT));
  params.set("keyword", value);
  const response = await fetch(`${API_BASE}/stocks/search?${params.toString()}`, {
    signal: controller.signal,
  });
  if (response.status === 404) {
    return fetchLegacySearchResults(value, controller);
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

async function performSearch(keyword) {
  const value = typeof keyword === "string" ? keyword.trim() : "";
  if (!value) {
    clearSearchInterface();
    return [];
  }
  const token = ++searchRequestToken;
  const controller = new AbortController();
  searchAbortController = controller;
  searchDebounceTimer = null;
  searchState.loading = true;
  renderSearchResults();
  try {
    const data = await fetchSearchResults(value, controller);
    if (controller.signal.aborted || token !== searchRequestToken) {
      return [];
    }
    const items = Array.isArray(data.items) ? data.items : [];
    searchState.results = items;
    searchState.loading = false;
    searchState.error = false;
    if (searchState.activeIndex >= items.length) {
      searchState.activeIndex = -1;
    }
    renderSearchResults();
    return items;
  } catch (error) {
    if (controller.signal.aborted || token !== searchRequestToken) {
      return [];
    }
    console.error("Stock search failed:", error);
    searchState.loading = false;
    searchState.error = true;
    searchState.results = [];
    renderSearchResults();
    return [];
  } finally {
    if (searchAbortController === controller) {
      searchAbortController = null;
    }
  }
}

function startImmediateSearch(keyword) {
  const value = typeof keyword === "string" ? keyword.trim() : "";
  if (!value) {
    clearSearchInterface();
    return Promise.resolve([]);
  }
  if (searchDebounceTimer) {
    clearTimeout(searchDebounceTimer);
    searchDebounceTimer = null;
  }
  if (searchAbortController) {
    searchAbortController.abort();
    searchAbortController = null;
  }
  searchState.keyword = value;
  searchState.activeIndex = -1;
  searchState.error = false;
  return performSearch(value);
}

function handleSearchInput(event) {
  const value = typeof event.target.value === "string" ? event.target.value : "";
  if (!value.trim()) {
    clearSearchInterface();
    return;
  }
  scheduleSearch(value);
}

async function handleSearchKeyDown(event) {
  if (event.key === "ArrowDown") {
    if (searchState.results.length) {
      event.preventDefault();
      if (searchState.activeIndex < searchState.results.length - 1) {
        searchState.activeIndex += 1;
      } else if (searchState.activeIndex < 0) {
        searchState.activeIndex = 0;
      }
      applySearchActiveState();
    }
    return;
  }
  if (event.key === "ArrowUp") {
    if (searchState.results.length) {
      event.preventDefault();
      if (searchState.activeIndex <= 0) {
        searchState.activeIndex = -1;
      } else {
        searchState.activeIndex -= 1;
      }
      applySearchActiveState();
    }
    return;
  }
  if (event.key === "Enter") {
    event.preventDefault();
    await executeSearch(event.target.value || "");
    return;
  }
  if (event.key === "Escape") {
    if (elements.searchInput && elements.searchInput.value) {
      event.preventDefault();
      elements.searchInput.value = "";
      clearSearchInterface();
    } else {
      hideSearchResults();
    }
  }
}

function handleSearchFocus() {
  if (searchBlurTimeout) {
    clearTimeout(searchBlurTimeout);
    searchBlurTimeout = null;
  }
  if (searchState.keyword) {
    renderSearchResults();
  }
}

function handleSearchBlur() {
  if (searchBlurTimeout) {
    clearTimeout(searchBlurTimeout);
  }
  searchBlurTimeout = window.setTimeout(() => {
    if (!elements.searchWrapper) {
      hideSearchResults();
      return;
    }
    const activeEl = document.activeElement;
    if (!elements.searchWrapper.contains(activeEl)) {
      hideSearchResults();
    }
  }, 150);
}

function handleSearchDocumentClick(event) {
  if (!elements.searchWrapper) {
    return;
  }
  if (!elements.searchWrapper.contains(event.target)) {
    hideSearchResults();
  }
}

async function executeSearch(rawValue) {
  const value = typeof rawValue === "string" ? rawValue.trim() : "";
  if (!value) {
    clearSearchInterface();
    if (elements.searchInput) {
      elements.searchInput.value = "";
    }
    return;
  }
  if (
    searchState.results.length &&
    value === searchState.keyword &&
    !searchState.loading
  ) {
    const index =
      searchState.activeIndex >= 0 && searchState.activeIndex < searchState.results.length
        ? searchState.activeIndex
        : 0;
    const candidate = searchState.results[index];
    if (candidate && candidate.code) {
      navigateToStock(candidate.code);
      return;
    }
  }
  const items = await startImmediateSearch(value);
  const list = Array.isArray(items) && items.length ? items : searchState.results;
  if (!list.length) {
    return;
  }
  const index =
    searchState.activeIndex >= 0 && searchState.activeIndex < list.length
      ? searchState.activeIndex
      : 0;
  const selected = list[index];
  if (selected && selected.code) {
    navigateToStock(selected.code);
  }
}

function navigateToStock(code) {
  if (!code) {
    return;
  }
  const normalized = String(code).trim();
  if (!normalized) {
    return;
  }
  const params = new URLSearchParams(window.location.search);
  params.set("code", normalized);
  const queryString = params.toString();
  const target = queryString ? `${window.location.pathname}?${queryString}` : window.location.pathname;
  window.location.href = target;
}

function initSearch() {
  if (!elements.searchInput || !elements.searchResults) {
    return;
  }
  elements.searchInput.addEventListener("input", handleSearchInput);
  elements.searchInput.addEventListener("keydown", handleSearchKeyDown);
  elements.searchInput.addEventListener("focus", handleSearchFocus);
  elements.searchInput.addEventListener("blur", handleSearchBlur);
  elements.searchInput.addEventListener("search", (event) => {
    executeSearch(event.target.value || "");
  });
  document.addEventListener("click", handleSearchDocumentClick);
}

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

function applyTranslations() {
  const dict = translations[currentLang];
  document.title = dict.pageTitle;
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      el.textContent = value;
    }
  });
  document.querySelectorAll("[data-placeholder-en]").forEach((el) => {
    const attr = `placeholder${currentLang.toUpperCase()}`;
    const placeholder = el.dataset[attr];
    if (typeof placeholder === "string") {
      el.placeholder = placeholder;
    }
  });
  if (elements.searchInput) {
    const label = dict.searchInputLabel;
    if (typeof label === "string" && label) {
      elements.searchInput.setAttribute("aria-label", label);
    }
  }
  if (elements.searchResults) {
    const resultsLabel = dict.searchResultsLabel;
    if (typeof resultsLabel === "string" && resultsLabel) {
      elements.searchResults.setAttribute("aria-label", resultsLabel);
    }
  }
  updateFavoriteToggle(favoriteState.isFavorite);
  renderPerformanceHighlights(performanceData);
  renderIndividualFundFlowCard(detailExtras.individualFundFlow);
  renderBigDealCard(detailExtras.bigDeals);
  if (currentDetail) {
    updateMainBusinessCard(currentDetail.businessProfile);
    updateMainCompositionCard(currentDetail.businessComposition);
  }
  renderSearchResults();
}

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(value);
}

function formatCompactNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatCurrency(value, { maximumFractionDigits = 0 } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: 0,
    maximumFractionDigits,
  }).format(value);
}

function formatPercent(value, { fromRatio = false } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const ratio = fromRatio ? value : value / 100;
  return `${(ratio * 100).toFixed(2)}%`;
}

function formatPercentFlexible(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const treatAsRatio = Math.abs(numeric) <= 1;
  const ratio = treatAsRatio ? numeric : numeric / 100;
  return `${(ratio * 100).toFixed(2)}%`;
}

function wrapSignedValue(value, display) {
  if (display === "--") {
    return display;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return display;
  }
  const needsPlus = numeric > 0 && typeof display === "string" && !display.startsWith("+");
  const text = needsPlus ? `+${display}` : display;
  const cls = numeric > 0 ? "text-up" : "text-down";
  return `<span class="${cls}">${text}</span>`;
}

function escapeHTML(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function truncateWithTooltip(text, limit = 120) {
  if (!text) {
    return { summary: "--", tooltip: "" };
  }
  const normalized = String(text).trim();
  if (normalized.length <= limit) {
    const escaped = escapeHTML(normalized);
    return { summary: escaped, tooltip: escaped };
  }
  const summary = escapeHTML(normalized.slice(0, limit)) + "…";
  return { summary, tooltip: escapeHTML(normalized) };
}

function renderTrendBadge(value) {
  if (value === null || value === undefined) {
    return `<span class="chip chip--neutral">--</span>`;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return `<span class="chip chip--neutral">--</span>`;
  }
  const cls = numeric > 0 ? "chip chip--positive" : numeric < 0 ? "chip chip--negative" : "chip chip--neutral";
  return `<span class="${cls}">${formatPercentFlexible(numeric)}</span>`;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return value;
  }
  return date.toISOString().slice(0, 10);
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return "--";
  }
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const min = String(date.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
}

function detectNumericTrend(value) {
  if (value === null || value === undefined) {
    return 0;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return value > 0 ? 1 : value < 0 ? -1 : 0;
  }
  const text = String(value).trim();
  if (!text || text === "--" || text === "-") {
    return 0;
  }
  const normalized = text.replace(/,/g, "");
  if (!/^[+-]?\d*(?:\.\d+)?%?$/.test(normalized)) {
    return 0;
  }
  const numeric = parseFloat(normalized.replace(/%$/, ""));
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  if (numeric > 0) {
    return 1;
  }
  if (numeric < 0) {
    return -1;
  }
  return 0;
}

function renderList(container, rows) {
  if (!container) {
    return;
  }
  container.innerHTML = rows
    .map(({ label, value, highlightTrend = false }) => {
      const displayLabel = escapeHTML(label ?? "--");
      const displayValue =
        value === null || value === undefined || value === "" ? "--" : escapeHTML(String(value));
      const trend = highlightTrend ? detectNumericTrend(value) : 0;
      const valueClasses = ["detail-row__value"];
      if (trend > 0) {
        valueClasses.push("text-up");
      } else if (trend < 0) {
        valueClasses.push("text-down");
      }
      return `
        <div class="detail-row">
          <span class="detail-row__label">${displayLabel}</span>
          <span class="${valueClasses.join(" ")}">${displayValue}</span>
        </div>
      `;
    })
    .join("");
}

function normalizeTsCode(value) {
  return (value || "").trim().toUpperCase();
}

function selectLatestRecord(items, code) {
  if (!Array.isArray(items) || !items.length) {
    return null;
  }
  const normalizedCode = normalizeTsCode(code);
  const normalizedSymbol = normalizedCode.split(".")[0];
  const enriched = items.map((entry) => {
    const tsCode = normalizeTsCode(entry.ts_code || entry.tsCode || "");
    const symbol = normalizeTsCode(entry.symbol || "");
    const announcement = parseTradeDate(
      entry.announcement_date || entry.ann_date || entry.announcementDate
    );
    const timestamp = announcement ? announcement.getTime() : 0;
    return { entry, tsCode, symbol, timestamp };
  });

  const filtered = enriched.filter(
    ({ tsCode, symbol }) =>
      tsCode === normalizedCode ||
      symbol === normalizedSymbol ||
      tsCode === normalizedSymbol
  );

  const target = filtered.length ? filtered : enriched;
  target.sort((a, b) => b.timestamp - a.timestamp);
  return target[0]?.entry ?? null;
}

function renderPerformanceHighlights({ express, forecast }) {
  if (!elements.performanceCard) {
    return;
  }

  const dict = translations[currentLang];
  const expressTile = elements.performanceExpressTile;
  const forecastTile = elements.performanceForecastTile;

  const hasExpress = Boolean(express);
  const hasForecast = Boolean(forecast);

  if (!hasExpress && !hasForecast) {
    elements.performanceCard.classList.add("hidden");
    if (expressTile) {
      expressTile.classList.add("hidden");
      expressTile.innerHTML = "";
    }
    if (forecastTile) {
      forecastTile.classList.add("hidden");
      forecastTile.innerHTML = "";
    }
    return;
  }

  elements.performanceCard.classList.remove("hidden");

  if (expressTile) {
    if (!hasExpress) {
      expressTile.innerHTML = "";
      expressTile.classList.add("hidden");
    } else {
      expressTile.classList.remove("hidden", "performance-tile--empty");
      const announcement = formatDate(
        express.announcement_date || express.ann_date || express.announcementDate
      );
      const period = formatDate(express.report_period || express.reportPeriod || express.end_date);
      const revenue = formatCompactNumber(express.revenue);
      const revenueYoyBadge = renderTrendBadge(
        express.revenue_yoy ?? express.revenueYearlyGrowth
      );
      const revenueQoqBadge = renderTrendBadge(
        express.revenue_qoq ?? express.revenueQuarterlyGrowth
      );
      const netProfit = formatCompactNumber(
        express.net_profit ?? express.netProfit ?? express.n_income
      );
      const netProfitYoyBadge = renderTrendBadge(
        express.net_profit_yoy ?? express.netProfitYearlyGrowth ?? express.yoy_net_profit
      );
      const netProfitQoqBadge = renderTrendBadge(
        express.net_profit_qoq ?? express.netProfitQuarterlyGrowth
      );
      const eps = formatNumber(express.eps ?? express.diluted_eps ?? express.dilutedEps, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      const roeBadge = renderTrendBadge(
        express.return_on_equity ?? express.returnOnEquity ?? express.diluted_roe
      );

      expressTile.innerHTML = `
        <div class="performance-tile__header">
          <h3 class="performance-tile__title">${escapeHTML(
            dict.performanceExpressHeading || "Performance Express"
          )}</h3>
          <span class="performance-tile__meta">${escapeHTML(
            dict.performancePanelAnnouncement || "Announcement"
          )} · ${announcement}</span>
        </div>
        <div class="performance-tile__metrics">
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceExpressRevenue || dict.labelRevenue || "Revenue"
            )}</span>
            <span class="performance-metric__value">${revenue}</span>
            <span class="performance-metric__delta">${revenueYoyBadge}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceExpressRevenueQoq || dict.labelRevenueQoqLatest || "Revenue QoQ"
            )}</span>
            <span class="performance-metric__delta">${revenueQoqBadge}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceExpressNetProfit || dict.labelNetIncome || "Net Profit"
            )}</span>
            <span class="performance-metric__value">${netProfit}</span>
            <span class="performance-metric__delta">${netProfitYoyBadge}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceExpressNetProfitQoq || dict.labelNetIncomeQoqLatest || "Net Profit QoQ"
            )}</span>
            <span class="performance-metric__delta">${netProfitQoqBadge}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceExpressEps || dict.labelBasicEps || "EPS"
            )}</span>
            <span class="performance-metric__value">${eps}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceExpressRoe || dict.labelRoe || "ROE"
            )}</span>
            <span class="performance-metric__delta">${roeBadge}</span>
          </div>
        </div>
        <div class="performance-tile__meta">${escapeHTML(
          dict.performancePanelPeriod || "Reporting Period"
        )} · ${period}</div>
      `;
    }
  }

  if (forecastTile) {
    if (!hasForecast) {
      forecastTile.innerHTML = "";
      forecastTile.classList.add("hidden");
    } else {
      forecastTile.classList.remove("hidden", "performance-tile--empty");
      const announcement = formatDate(
        forecast.announcement_date || forecast.ann_date || forecast.announcementDate
      );
      const period = formatDate(forecast.report_period || forecast.reportPeriod || forecast.end_date);
      const metric = escapeHTML(forecast.forecast_metric || forecast.forecastMetric || "--");
      const type = escapeHTML(forecast.forecast_type || forecast.type || "--");
      const forecastValue = formatCompactNumber(forecast.forecast_value ?? forecast.forecastValue);
      const lastYear = formatCompactNumber(forecast.last_year_value ?? forecast.lastYearValue);
      const changeBadge = renderTrendBadge(forecast.change_rate ?? forecast.changeRate);
      const rawDescription = forecast.change_description ?? forecast.changeDescription ?? "";
      const rawReason = forecast.change_reason ?? forecast.changeReason ?? "";
      const descriptionText =
        rawDescription && String(rawDescription).trim()
          ? escapeHTML(String(rawDescription).trim())
          : "--";
      const reasonText =
        rawReason && String(rawReason).trim()
          ? escapeHTML(String(rawReason).trim())
          : "--";

      forecastTile.innerHTML = `
        <div class="performance-tile__header">
          <h3 class="performance-tile__title">${escapeHTML(
            dict.performanceForecastHeading || "Performance Forecast"
          )}</h3>
          <span class="performance-tile__meta">${escapeHTML(
            dict.performancePanelAnnouncement || "Announcement"
          )} · ${announcement}</span>
        </div>
        <div class="performance-tile__metrics">
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceForecastMetric || "Metric"
            )}</span>
            <span class="performance-metric__value">${metric}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceForecastType || "Type"
            )}</span>
            <span class="performance-metric__value">${type}</span>
            <span class="performance-metric__delta">${changeBadge}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceForecastValue || "Forecast"
            )}</span>
            <span class="performance-metric__value">${forecastValue}</span>
          </div>
          <div class="performance-metric">
            <span class="performance-metric__label">${escapeHTML(
              dict.performanceForecastLastYear || "Last Year"
            )}</span>
            <span class="performance-metric__value">${lastYear}</span>
          </div>
        </div>
        <p class="performance-tile__description">${descriptionText}</p>
        <p class="performance-tile__description">${reasonText}</p>
        <div class="performance-tile__meta">${escapeHTML(
          dict.performancePanelPeriod || "Reporting Period"
        )} · ${period}</div>
      `;
    }
  }
}

function hideIndividualFundFlowCard() {
  if (!elements.individualFundFlowCard) {
    return;
  }
  elements.individualFundFlowCard.classList.add("hidden");
  if (elements.individualFundFlowList) {
    elements.individualFundFlowList.innerHTML = "";
  }
  detailExtras.individualFundFlow = [];
}

function renderIndividualFundFlowCard(entries) {
  if (!elements.individualFundFlowCard || !elements.individualFundFlowList) {
    return;
  }
  const items = Array.isArray(entries) ? entries : [];
  if (!items.length) {
    hideIndividualFundFlowCard();
    return;
  }
  const dict = translations[currentLang];
  const sections = INDIVIDUAL_FUND_FLOW_SYMBOLS.map(({ match, key, label, translationKey }) => {
    const matches = items
      .filter((candidate) => {
        const symbol = candidate.symbol || candidate.Symbol || candidate.symbol_name;
        if (!symbol) {
          return false;
        }
        const normalized = String(symbol).trim();
        return normalized === match || normalized === key;
      })
      .sort((a, b) => {
        const rankA = Number.isFinite(Number(a.rank)) ? Number(a.rank) : Number.POSITIVE_INFINITY;
        const rankB = Number.isFinite(Number(b.rank)) ? Number(b.rank) : Number.POSITIVE_INFINITY;
        return rankA - rankB;
      });
    const entry = matches[0] || null;
    const variant = key === "symbolInstant" || match === "即时" ? "instant" : "ranked";
    const metrics = entry ? buildIndividualFundFlowMetrics(entry, variant, dict) : [];
    const heading = (translationKey && dict[translationKey]) || dict[key] || label || match;
    return { symbol: match, key, label: heading, entry, metrics };
  });

  const hasAny = sections.some(({ metrics }) => metrics.length > 0);
  if (!hasAny) {
    hideIndividualFundFlowCard();
    return;
  }

  const content = sections
    .filter(({ metrics }) => metrics.length > 0)
    .map(({ label, entry, metrics }) => {
      const updatedAtRaw = getRecordValue(entry, "updatedAt", "updated_at");
      const updatedAt = formatDateTime(updatedAtRaw);
      const updatedLabel = escapeHTML(dict.individualFundFlowUpdatedAt || "Updated");
      const metricsMarkup = metrics
        .map(
          (metric) => `
            <div class="individual-fund-flow-metric">
              <span class="individual-fund-flow-metric__label">${escapeHTML(metric.label)}</span>
              <span class="individual-fund-flow-metric__value">${metric.value}</span>
            </div>
          `
        )
        .join("");

      return `
        <div class="individual-fund-flow-row">
          <div class="individual-fund-flow-row__meta">
            <span class="individual-fund-flow-row__label">${escapeHTML(label)}</span>
            <span class="individual-fund-flow-row__update">${updatedLabel}：${updatedAt}</span>
          </div>
          <div class="individual-fund-flow-row__metrics">
            ${metricsMarkup}
          </div>
        </div>
      `;
    })
    .join("");

  elements.individualFundFlowList.innerHTML = `<div class="individual-fund-flow-horizontal">${content}</div>`;
  elements.individualFundFlowCard.classList.remove("hidden");
  detailExtras.individualFundFlow = items;
}

async function loadIndividualFundFlowData(code) {
  if (!elements.individualFundFlowCard) {
    return;
  }
  const normalized = normalizeCode(code);
  if (!normalized) {
    hideIndividualFundFlowCard();
    return;
  }
  hideIndividualFundFlowCard();

  try {
    const response = await fetch(
      `${API_BASE}/fund-flow/individual?limit=100&code=${encodeURIComponent(normalized)}`
    );
    if (!response.ok) {
      hideIndividualFundFlowCard();
      return;
    }
    const data = await response.json();
    const items = Array.isArray(data.items) ? data.items : [];
    renderIndividualFundFlowCard(items);
  } catch (error) {
    console.error("Failed to load individual fund flow data:", error);
    hideIndividualFundFlowCard();
  }
}

function translateDealSide(value) {
  const raw = (value || "").trim();
  if (!raw) {
    return "--";
  }
  const dict = translations[currentLang];
  const normalized = raw.toLowerCase();
  if (normalized.includes("买")) {
    return dict.bigDealSideBuy || "Buy";
  }
  if (normalized.includes("sell") || normalized.includes("卖")) {
    return dict.bigDealSideSell || "Sell";
  }
  return raw;
}

function hideBigDealCard() {
  if (!elements.bigDealCard) {
    return;
  }
  elements.bigDealCard.classList.add("hidden");
  if (elements.bigDealList) {
    elements.bigDealList.innerHTML = "";
  }
  detailExtras.bigDeals = [];
}

function renderBigDealCard(items) {
  if (!elements.bigDealCard || !elements.bigDealList) {
    return;
  }
  if (!items || !items.length) {
    hideBigDealCard();
    return;
  }
  const dict = translations[currentLang];
  const subset = items.slice(0, 5);
  const content = subset
    .map((item) => {
      const time = formatDateTime(item.trade_time || item.tradeTime);
      const side = escapeHTML(translateDealSide(item.trade_side || item.tradeSide));
      const sideRaw = (item.trade_side || item.tradeSide || "").trim();
      const price = formatNumber(item.trade_price || item.tradePrice, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      const amount = formatCurrency(item.trade_amount || item.tradeAmount, { maximumFractionDigits: 0 });
      const volume = formatCurrency(item.trade_volume || item.tradeVolume, { maximumFractionDigits: 0 });
      const changePct = formatPercentFlexible(item.price_change_percent || item.priceChangePercent);
      const change = formatNumber(item.price_change || item.priceChange, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      const codeParts = deriveCodeParts(item.stock_code || item.stockCode);
      const stockName = escapeHTML(item.stock_name || item.stockName || "--");
      const codeLink = codeParts.detailCode
        ? `<a class="link big-deal-row__code" href="stock-detail.html?code=${encodeURIComponent(
            codeParts.detailCode
          )}" target="_blank" rel="noopener noreferrer">${escapeHTML(codeParts.displayCode)}</a>`
        : `<span class="big-deal-row__code">--</span>`;
      const nameLink = codeParts.detailCode
        ? `<a class="link big-deal-row__name" href="stock-detail.html?code=${encodeURIComponent(
            codeParts.detailCode
          )}" target="_blank" rel="noopener noreferrer">${stockName}</a>`
        : `<span class="big-deal-row__name">${stockName}</span>`;
      const changePctContent = wrapSignedValue(item.price_change_percent || item.priceChangePercent, changePct);
      const changeAmountContent = wrapSignedValue(item.price_change || item.priceChange, change);
      const rowClass =
        sideRaw && sideRaw.includes("买")
          ? "big-deal-row__side--buy"
          : sideRaw && sideRaw.toLowerCase().includes("sell")
          ? "big-deal-row__side--sell"
          : sideRaw && sideRaw.includes("卖")
          ? "big-deal-row__side--sell"
          : "";

      return `
        <div class="big-deal-row">
          <div class="big-deal-row__meta">
            <span class="big-deal-row__time">${time}</span>
            <div class="big-deal-row__identity">
              ${codeLink}
              <span class="big-deal-row__separator">·</span>
              ${nameLink}
            </div>
            <span class="big-deal-row__side ${rowClass}">${side}</span>
          </div>
          <div class="big-deal-row__metrics">
            <div class="big-deal-metric">
              <span class="big-deal-metric__label">${escapeHTML(dict.bigDealColumnPrice || "Price")}</span>
              <span class="big-deal-metric__value">${price}</span>
            </div>
            <div class="big-deal-metric">
              <span class="big-deal-metric__label">${escapeHTML(dict.bigDealColumnAmount || "Amount")}</span>
              <span class="big-deal-metric__value">${amount}</span>
            </div>
            <div class="big-deal-metric">
              <span class="big-deal-metric__label">${escapeHTML(dict.bigDealColumnVolume || "Volume")}</span>
              <span class="big-deal-metric__value">${volume}</span>
            </div>
            <div class="big-deal-metric">
              <span class="big-deal-metric__label">${escapeHTML(dict.bigDealColumnChange || "Change")}</span>
              <span class="big-deal-metric__value">${changeAmountContent}</span>
            </div>
            <div class="big-deal-metric">
              <span class="big-deal-metric__label">${escapeHTML(
                dict.bigDealColumnChangePercent || "Change (%)"
              )}</span>
              <span class="big-deal-metric__value">${changePctContent}</span>
            </div>
          </div>
        </div>
      `;
    })
    .join("");

  elements.bigDealList.innerHTML = `<div class="big-deal-horizontal">${content}</div>`;
  elements.bigDealCard.classList.remove("hidden");
  detailExtras.bigDeals = subset;
}

async function loadBigDealData(code) {
  if (!elements.bigDealCard) {
    return;
  }
  const normalized = normalizeCode(code);
  if (!normalized) {
    hideBigDealCard();
    return;
  }
  hideBigDealCard();
  try {
    const response = await fetch(
      `${API_BASE}/fund-flow/big-deal?limit=5&code=${encodeURIComponent(normalized)}`
    );
    if (!response.ok) {
      hideBigDealCard();
      return;
    }
    const data = await response.json();
    const items = Array.isArray(data.items) ? data.items : [];
    renderBigDealCard(items);
  } catch (error) {
    console.error("Failed to load big deal data:", error);
    hideBigDealCard();
  }
}

async function loadPerformanceData(code) {
  if (!elements.performanceCard) {
    return;
  }
  const dict = translations[currentLang];
  if (elements.performanceExpressTile) {
    elements.performanceExpressTile.innerHTML = `<p class="performance-tile__empty">${escapeHTML(
      dict.loading || "Loading..."
    )}</p>`;
    elements.performanceExpressTile.classList.add("performance-tile--empty");
    elements.performanceExpressTile.classList.remove("hidden");
  }
  if (elements.performanceForecastTile) {
    elements.performanceForecastTile.innerHTML = `<p class="performance-tile__empty">${escapeHTML(
      dict.loading || "Loading..."
    )}</p>`;
    elements.performanceForecastTile.classList.add("performance-tile--empty");
    elements.performanceForecastTile.classList.remove("hidden");
  }

  try {
    const normalizedCode = normalizeCode(code);
    const rawCode = (code || "").trim().toUpperCase();
    const tokens = [];
    const addToken = (token) => {
      const candidate = (token || "").trim();
      if (candidate && !tokens.includes(candidate)) {
        tokens.push(candidate);
      }
    };

    addToken(normalizedCode);

    const normalizedSymbol = normalizedCode.includes(".")
      ? normalizedCode.split(".")[0]
      : normalizedCode.replace(/^(SH|SZ|BJ)/, "");
    addToken(normalizedSymbol);
    addToken(rawCode);

    if (/^\d{6}$/.test(normalizedSymbol)) {
      addToken(`${normalizedSymbol}.SH`);
      addToken(`${normalizedSymbol}.SZ`);
      addToken(`${normalizedSymbol}.BJ`);
    }

    let expressJson = { items: [] };
    let forecastJson = { items: [] };

    for (const token of tokens) {
      const encoded = encodeURIComponent(token);
      const [expressRes, forecastRes] = await Promise.all([
        fetch(`${API_BASE}/performance/express?limit=20&keyword=${encoded}`),
        fetch(`${API_BASE}/performance/forecast?limit=20&keyword=${encoded}`),
      ]);

      expressJson = expressRes.ok ? await expressRes.json() : { items: [] };
      forecastJson = forecastRes.ok ? await forecastRes.json() : { items: [] };

      const hasExpress = Array.isArray(expressJson.items) && expressJson.items.length > 0;
      const hasForecast = Array.isArray(forecastJson.items) && forecastJson.items.length > 0;

      if (hasExpress || hasForecast) {
        break;
      }
    }

    const selectionTarget =
      normalizedCode ||
      tokens.find((token) => token.includes(".")) ||
      tokens[0] ||
      "";

    const expressRecord = selectLatestRecord(expressJson.items || [], selectionTarget);
    const forecastRecord = selectLatestRecord(forecastJson.items || [], selectionTarget);

    performanceData = { express: expressRecord, forecast: forecastRecord };
  } catch (error) {
    console.error("Failed to load performance highlights:", error);
    performanceData = { express: null, forecast: null };
  }

  renderPerformanceHighlights(performanceData);
}

function parseTradeDate(value) {
  if (!value) {
    return null;
  }
  let normalized = value;
  if (typeof normalized === "number") {
    normalized = String(normalized);
  }
  if (typeof normalized !== "string") {
    return null;
  }
  const trimmed = normalized.trim();
  if (/^\d{8}$/.test(trimmed)) {
    const year = Number.parseInt(trimmed.slice(0, 4), 10);
    const month = Number.parseInt(trimmed.slice(4, 6), 10) - 1;
    const day = Number.parseInt(trimmed.slice(6, 8), 10);
    return new Date(Date.UTC(year, month, day));
  }
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    const date = new Date(trimmed);
    if (Number.isFinite(date.getTime())) {
      return date;
    }
  }
  const date = new Date(trimmed);
  return Number.isFinite(date.getTime()) ? date : null;
}

function limitCandlestickRange(data) {
  if (!Array.isArray(data) || !data.length) {
    return [];
  }
  let latest = null;
  data.forEach((item) => {
    const parsed = parseTradeDate(item?.time);
    if (parsed && (!latest || parsed > latest)) {
      latest = parsed;
    }
  });
  if (!latest) {
    return data.slice(-252);
  }
  const cutoff = new Date(latest);
  cutoff.setUTCDate(cutoff.getUTCDate() - 365);
  const filtered = data.filter((item) => {
    const parsed = parseTradeDate(item?.time);
    return !parsed || parsed >= cutoff;
  });
  if (filtered.length) {
    return filtered;
  }
  return data.slice(-252);
}

function ensureCandlestickChart() {
  if (!window.echarts || !elements.candlestickContainer) {
    return null;
  }
  if (!candlestickChartInstance) {
    candlestickChartInstance = window.echarts.init(elements.candlestickContainer);
  }
  return candlestickChartInstance;
}

function hideCandlestickChart() {
  if (candlestickRenderTimeout) {
    window.clearTimeout(candlestickRenderTimeout);
    candlestickRenderTimeout = null;
  }
  if (candlestickChartInstance) {
    candlestickChartInstance.clear();
  }
  if (elements.candlestickContainer) {
    elements.candlestickContainer.classList.add("hidden");
  }
  if (elements.candlestickEmpty) {
    elements.candlestickEmpty.classList.remove("hidden");
  }
}

function renderCandlestickChart() {
  if (candlestickRenderTimeout) {
    window.clearTimeout(candlestickRenderTimeout);
    candlestickRenderTimeout = null;
  }

  if (!candlestickData.length) {
    hideCandlestickChart();
    return;
  }

  if (!window.echarts) {
    candlestickRenderTimeout = window.setTimeout(() => {
      candlestickRenderTimeout = null;
      renderCandlestickChart();
    }, 150);
    if (elements.candlestickContainer) {
      elements.candlestickContainer.classList.add("hidden");
    }
    if (elements.candlestickEmpty) {
      elements.candlestickEmpty.classList.remove("hidden");
    }
    return;
  }

  const chart = ensureCandlestickChart();
  if (!chart) {
    return;
  }

  elements.candlestickContainer.classList.remove("hidden");
  elements.candlestickEmpty.classList.add("hidden");

  const dict = translations[currentLang];
  const categories = candlestickData.map((item) => {
    const formatted = formatDate(item.time);
    return formatted === "--" ? item.time : formatted;
  });
  const upColor = "#3066BE"; // blue for price increases
  const downColor = "#E07A1F"; // orange for price decreases
  const seriesData = candlestickData.map((item) => [
    item.open,
    item.close,
    item.low,
    item.high,
  ]);
  const volumeSeries = candlestickData.map((item) => {
    const volumeValue =
      item.volume === null || item.volume === undefined || Number.isNaN(item.volume)
        ? 0
        : Number(item.volume);
    const rising = item.close >= item.open;
    return {
      value: volumeValue,
      itemStyle: { color: rising ? upColor : downColor },
    };
  });

  chart.setOption(
    {
      animation: false,
      axisPointer: {
        link: [{ xAxisIndex: [0, 1] }],
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross", snap: false },
        backgroundColor: "rgba(248, 250, 252, 0.95)",
        borderColor: "rgba(148, 163, 184, 0.4)",
        textStyle: { color: "#0f172a" },
        extraCssText: "box-shadow:0 12px 32px rgba(15,23,42,0.18); border-radius:12px; padding:12px 16px;",
        formatter: (params) => {
          if (!Array.isArray(params) || !params.length) {
            return "";
          }
          const [candlestickPoint] = params;
          const index = candlestickPoint?.dataIndex ?? 0;
          const source = candlestickData[index];
          const open = source?.open ?? candlestickPoint?.data?.[0];
          const close = source?.close ?? candlestickPoint?.data?.[1];
          const low = source?.low ?? candlestickPoint?.data?.[2];
          const high = source?.high ?? candlestickPoint?.data?.[3];
          let pctText = '<span class="tooltip-row__value">--</span>';
          if (typeof open === "number" && typeof close === "number" && open) {
            const pct = ((close - open) / open) * 100;
            if (Number.isFinite(pct)) {
              const pctDisplay = `${pct > 0 ? "+" : ""}${pct.toFixed(2)}%`;
              pctText =
                pct > 0
                  ? `<span class="tooltip-row__value tooltip-row__value--up">${pctDisplay}</span>`
                  : pct < 0
                  ? `<span class="tooltip-row__value tooltip-row__value--down">${pctDisplay}</span>`
                  : `<span class="tooltip-row__value">${pctDisplay}</span>`;
            }
          }
          const header = `<div class="tooltip-title">${candlestickPoint?.axisValueLabel ?? ""}</div>`;
          const lines = [
            {
              label: dict.labelOpen ?? "Open",
              value: escapeHTML(
                formatNumber(open, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
              ),
            },
            {
              label: dict.labelClose ?? "Close",
              value: escapeHTML(
                formatNumber(close, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
              ),
            },
            {
              label: dict.labelLow ?? "Low",
              value: escapeHTML(
                formatNumber(low, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
              ),
            },
            {
              label: dict.labelHigh ?? "High",
              value: escapeHTML(
                formatNumber(high, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
              ),
            },
            { label: dict.labelChange ?? "Change", value: pctText, isHtml: true },
          ];
          const rows = lines
            .map(
              ({ label, value, isHtml }) => `
                <div class="tooltip-row">
                  <span class="tooltip-row__label">${escapeHTML(label)}</span>
                  ${isHtml ? value : `<span class="tooltip-row__value">${value}</span>`}
                </div>
              `
            )
            .join("");
          return `<div class="tooltip-card">${header}${rows}</div>`;
        },
      },
      grid: [
        { left: 40, right: 16, top: 16, height: "56%" },
        { left: 40, right: 16, top: "76%", height: "18%" },
      ],
      xAxis: [
        {
          type: "category",
          scale: true,
          boundaryGap: true,
          data: categories,
          axisLine: { lineStyle: { color: "#cbd5f5" } },
          axisTick: { alignWithLabel: true, show: false },
          axisLabel: { color: "#64748b" },
          splitLine: { show: false },
        },
        {
          type: "category",
          gridIndex: 1,
          scale: true,
          boundaryGap: true,
          data: categories,
          axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.4)" } },
          axisTick: { show: false },
          axisLabel: { color: "#94a3b8" },
          splitLine: { show: false },
        },
      ],
      yAxis: [
        {
          scale: true,
          axisLine: { show: false },
          axisLabel: { color: "#64748b" },
          splitLine: { lineStyle: { color: "#e5e7eb" } },
          min: (value) => value.min,
        },
        {
          gridIndex: 1,
          scale: true,
          axisLine: { show: false },
          axisLabel: {
            color: "#94a3b8",
            formatter: (value) => formatNumber(value, { notation: "compact" }),
          },
          splitLine: { show: false },
        },
      ],
      dataZoom: [
        { type: "inside", xAxisIndex: [0, 1], start: 60, end: 100, minSpan: 5 },
        {
          type: "slider",
          start: 60,
          end: 100,
          height: 24,
          bottom: 12,
          borderColor: "rgba(148, 163, 184, 0.4)",
          handleSize: 16,
          xAxisIndex: [0, 1],
        },
      ],
      series: [
        {
          name: dict.candlestickTitle,
          type: "candlestick",
          xAxisIndex: 0,
          yAxisIndex: 0,
          data: seriesData,
          itemStyle: {
            color: upColor,
            color0: downColor,
          borderColor: upColor,
          borderColor0: downColor,
          },
        },
        {
          name: dict.labelVolume,
          type: "bar",
          xAxisIndex: 1,
          yAxisIndex: 1,
          data: volumeSeries,
          barWidth: "60%",
        },
      ],
    },
    true
  );

  chart.resize();
}

function resizeCandlestickChart() {
  if (candlestickChartInstance) {
    candlestickChartInstance.resize();
  }
}

function renderDetail(detail) {
  currentDetail = detail;
  const dict = translations[currentLang];
  favoriteState.code = normalizeCode(detail.profile.code);
  favoriteState.group = normalizeFavoriteGroupValue(
    detail.favoriteGroup ??
      detail.profile?.favoriteGroup ??
      favoriteState.group ??
      null
  );
  const isFavorite = Boolean(
    detail.isFavorite ?? detail.profile?.isFavorite ?? favoriteState.isFavorite
  );
  updateFavoriteToggle(isFavorite);
  setFavoriteBusy(false);
  currentDetail.isFavorite = isFavorite;
  if (currentDetail.profile) {
    currentDetail.profile.isFavorite = isFavorite;
    currentDetail.profile.favoriteGroup = favoriteState.group;
  }
  currentDetail.favoriteGroup = favoriteState.group;
  elements.title.textContent = detail.profile.name ?? detail.profile.code;
  elements.subtitle.textContent = detail.profile.code;
  updateExternalLinks(detail.profile.code);
  updateMainBusinessCard(detail.businessProfile);
  updateMainCompositionCard(detail.businessComposition);
  document.title = `${detail.profile.code} · ${dict.pageTitle}`;

  const lastPrice = detail.tradingData.lastPrice ?? detail.profile.lastPrice;
  elements.heroPrice.textContent = formatNumber(lastPrice, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

  const pctChange = detail.tradingData.pctChange ?? detail.profile.pctChange;
  elements.heroChange.classList.remove("detail-hero__change--up", "detail-hero__change--down");
  let changeText = "--";
  if (pctChange !== null && pctChange !== undefined && !Number.isNaN(pctChange)) {
    const absChange =
      lastPrice !== null && lastPrice !== undefined && !Number.isNaN(lastPrice)
        ? (lastPrice * pctChange) / 100
        : null;
    const formattedAbs =
      absChange === null
        ? null
        : `${absChange >= 0 ? "+" : ""}${formatNumber(absChange, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}`;
    const formattedPct = `${pctChange >= 0 ? "+" : ""}${pctChange.toFixed(2)}%`;
    changeText = [formattedAbs, formattedPct].filter(Boolean).join(" ");
    if (pctChange > 0) {
      elements.heroChange.classList.add("detail-hero__change--up");
    } else if (pctChange < 0) {
      elements.heroChange.classList.add("detail-hero__change--down");
    }
  }
  elements.heroChange.textContent = changeText;

  const metaParts = [detail.profile.market, detail.profile.exchange, detail.profile.industry].filter(Boolean);
  elements.heroMeta.textContent = metaParts.join(" · ") || "--";

  const updatedLabel = detail.profile.tradeDate
    ? dict.updatedAt.replace("{date}", formatDate(detail.profile.tradeDate))
    : "";
  elements.heroUpdated.textContent = updatedLabel;
  elements.heroUpdated.classList.toggle("hidden", !updatedLabel);

  elements.heroMarketCap.textContent = formatCompactNumber(
    detail.tradingData.marketCap ?? detail.profile.marketCap
  );
  elements.heroVolume.textContent = formatCompactNumber(
    detail.tradingData.volume ?? detail.profile.volume
  );
  elements.heroPe.textContent = formatNumber(detail.tradingData.peRatio ?? detail.profile.peRatio, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  elements.heroTurnover.textContent = formatPercent(
    detail.tradingData.turnoverRate ?? detail.profile.turnoverRate
  );

  renderList(elements.financialList, [
    {
      label: dict.labelAnnDate,
      value: formatDate(detail.financialData.annDate),
    },
    {
      label: dict.labelEndDate,
      value: formatDate(detail.financialData.endDate),
    },
    {
      label: dict.labelBasicEps,
      value: formatNumber(detail.financialData.basicEps, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    },
    {
      label: dict.labelRevenue,
      value: formatNumber(
        detail.financialData.revenue ? detail.financialData.revenue / 1_000_000 : null,
        { maximumFractionDigits: 2 }
      ),
    },
    {
      label: dict.labelOperateProfit,
      value: formatNumber(
        detail.financialData.operateProfit
          ? detail.financialData.operateProfit / 1_000_000
          : null,
        { maximumFractionDigits: 2 }
      ),
    },
    {
      label: dict.labelNetIncome,
      value: formatNumber(
        detail.financialData.netIncome ? detail.financialData.netIncome / 1_000_000 : null,
        { maximumFractionDigits: 2 }
      ),
    },
    {
      label: dict.labelGrossMargin,
      value: formatNumber(
        detail.financialData.grossMargin ? detail.financialData.grossMargin / 1_000_000 : null,
        { maximumFractionDigits: 2 }
      ),
    },
    {
      label: dict.labelRoe,
      value: formatPercentFlexible(detail.financialData.roe),
      highlightTrend: true,
    },
  ]);

  renderList(elements.statsList, [
    {
      label: dict.labelPct1Y,
      value: formatPercent(detail.tradingStats.pctChange1Y, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelPct6M,
      value: formatPercent(detail.tradingStats.pctChange6M, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelPct3M,
      value: formatPercent(detail.tradingStats.pctChange3M, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelPct1M,
      value: formatPercent(detail.tradingStats.pctChange1M, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelPct2W,
      value: formatPercent(detail.tradingStats.pctChange2W, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelPct1W,
      value: formatPercent(detail.tradingStats.pctChange1W, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelVolumeSpike,
      value: formatNumber(detail.tradingStats.volumeSpike, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    },
    {
      label: dict.labelMa20,
      value: formatNumber(detail.tradingStats.ma20, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    },
    {
      label: dict.labelMa10,
      value: formatNumber(detail.tradingStats.ma10, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    },
    {
      label: dict.labelMa5,
      value: formatNumber(detail.tradingStats.ma5, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }),
    },
  ]);

  renderList(elements.fundamentalsList, [
    {
      label: dict.labelReportingPeriod,
      value: formatDate(detail.financialStats.reportingPeriod),
    },
    {
      label: dict.labelNetIncomeYoyLatest,
      value: formatPercent(detail.financialStats.netIncomeYoyLatest, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelNetIncomeYoyPrev1,
      value: formatPercent(detail.financialStats.netIncomeYoyPrev1, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelNetIncomeYoyPrev2,
      value: formatPercent(detail.financialStats.netIncomeYoyPrev2, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelNetIncomeQoqLatest,
      value: formatPercent(detail.financialStats.netIncomeQoqLatest, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelRevenueYoyLatest,
      value: formatPercent(detail.financialStats.revenueYoyLatest, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelRevenueQoqLatest,
      value: formatPercent(detail.financialStats.revenueQoqLatest, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelRoeYoyLatest,
      value: formatPercent(detail.financialStats.roeYoyLatest, { fromRatio: true }),
      highlightTrend: true,
    },
    {
      label: dict.labelRoeQoqLatest,
      value: formatPercent(detail.financialStats.roeQoqLatest, { fromRatio: true }),
      highlightTrend: true,
    },
  ]);

  candlestickData = limitCandlestickRange(detail.dailyTradeHistory || []);
  renderCandlestickChart();

  renderPerformanceHighlights(performanceData);
}

function setStatus(messageKey, isError = false) {
  const dict = translations[currentLang];
  elements.status.textContent = dict[messageKey] || messageKey;
  elements.status.classList.toggle("detail-status--error", isError);
  elements.status.classList.remove("hidden");
  elements.hero.classList.add("hidden");
  elements.grid.classList.add("hidden");
  if (elements.performanceCard) {
    elements.performanceCard.classList.add("hidden");
  }
  hideIndividualFundFlowCard();
  hideBigDealCard();
  performanceData = { express: null, forecast: null };
  favoriteState.code = null;
  favoriteState.group = null;
  updateFavoriteToggle(false);
  setFavoriteBusy(true);
  if (candlestickChartInstance) {
    candlestickChartInstance.clear();
  }
  if (elements.candlestickContainer) {
    elements.candlestickContainer.classList.add("hidden");
  }
  if (elements.candlestickEmpty) {
    elements.candlestickEmpty.classList.add("hidden");
  }
  if (candlestickData && candlestickData.length) {
    renderCandlestickChart();
  }
}

function showDetail() {
  elements.status.classList.add("hidden");
  elements.grid.classList.remove("hidden");
  elements.hero.classList.remove("hidden");
  if (elements.candlestickEmpty) {
    elements.candlestickEmpty.classList.add("hidden");
  }
  if (candlestickData && candlestickData.length) {
    renderCandlestickChart();
  }
  resizeCandlestickChart();
}

async function fetchDetail(code) {
  try {
    setStatus("loading");
    const response = await fetch(`${API_BASE}/stocks/${encodeURIComponent(code)}`);
    if (!response.ok) {
      if (response.status === 404) {
        setStatus("errorNotFound", true);
        return;
      }
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    performanceData = { express: null, forecast: null };
    renderDetail(data);
    const normalizedCode = data?.profile?.code || code;
    loadPerformanceData(normalizedCode);
    loadIndividualFundFlowData(normalizedCode);
    loadBigDealData(normalizedCode);
    showDetail();
  } catch (error) {
    console.error("Failed to load stock detail:", error);
    setStatus("errorGeneric", true);
  }
}

function handleLanguageSwitch(lang) {
  if (!translations[lang]) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  applyTranslations();
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === lang);
  });
  if (currentDetail) {
    renderDetail(currentDetail);
    showDetail();
  }
  renderPerformanceHighlights(performanceData);
  renderIndividualFundFlowCard(detailExtras.individualFundFlow);
  renderBigDealCard(detailExtras.bigDeals);
  renderCandlestickChart();
}

function initLanguageButtons() {
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
    btn.addEventListener("click", () => handleLanguageSwitch(btn.dataset.lang));
  });
}

function initialize() {
  applyTranslations();
  initLanguageButtons();
  initFavoriteToggle();
  initSearch();
  const params = new URLSearchParams(window.location.search);
  const code = params.get("code");
  if (!code) {
    setStatus("errorNoCode", true);
    return;
  }
  document.title = `${translations[currentLang].pageTitle} - ${code}`;
  fetchDetail(code);
  window.addEventListener("resize", resizeCandlestickChart);
}


window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
