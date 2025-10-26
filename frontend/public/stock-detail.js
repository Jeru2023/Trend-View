const translations = getTranslations("stockDetail");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const LANG_STORAGE_KEY = "trend-view-lang";

let currentLang = document.documentElement.getAttribute("data-pref-lang") || getInitialLanguage();
let candlestickData = [];
let currentDetail = null;
let candlestickChartInstance = null;
let candlestickRenderTimeout = null;

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
  profileList: document.getElementById("profile-list"),
  financialList: document.getElementById("financial-list"),
  statsList: document.getElementById("stats-list"),
  fundamentalsList: document.getElementById("fundamentals-list"),
  langButtons: document.querySelectorAll(".lang-btn"),
  candlestickContainer: document.getElementById("candlestick-chart"),
  candlestickEmpty: document.getElementById("candlestick-empty"),
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
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
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

function formatPercent(value, { fromRatio = false } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const ratio = fromRatio ? value : value / 100;
  return `${(ratio * 100).toFixed(2)}%`;
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

function renderList(container, rows) {
  container.innerHTML = rows
    .map(({ label, value }) => {
      const displayLabel = label ?? "--";
      const displayValue =
        value === null || value === undefined || value === "" ? "--" : value;
      return `
      <dt>${displayLabel}</dt>
      <dd>${displayValue}</dd>
    `;
    })
    .join("");
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
  const upColor = "#16a34a";
  const downColor = "#dc2626";
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
        backgroundColor: "rgba(17,23,39,0.9)",
      },
      grid: [
        { left: 40, right: 16, top: 16, height: "60%" },
        { left: 40, right: 16, top: "70%", height: "24%" },
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
  elements.title.textContent = detail.profile.name ?? detail.profile.code;
  elements.subtitle.textContent = detail.profile.code;
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

  renderList(elements.profileList, [
    { label: dict.labelCode, value: detail.profile.code },
    { label: dict.labelName, value: detail.profile.name ?? "--" },
    { label: dict.labelIndustry, value: detail.profile.industry ?? "--" },
    { label: dict.labelMarket, value: detail.profile.market ?? "--" },
    { label: dict.labelExchange, value: detail.profile.exchange ?? "--" },
    { label: dict.labelStatus, value: detail.profile.status ?? "--" },
    {
      label: dict.labelTradeDate,
      value: formatDate(detail.profile.tradeDate),
    },
  ]);

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
      value: formatPercent(detail.financialData.roe, { fromRatio: true }),
    },
  ]);

  renderList(elements.statsList, [
    { label: dict.labelPct1Y, value: formatPercent(detail.tradingStats.pctChange1Y, { fromRatio: true }) },
    { label: dict.labelPct6M, value: formatPercent(detail.tradingStats.pctChange6M, { fromRatio: true }) },
    { label: dict.labelPct3M, value: formatPercent(detail.tradingStats.pctChange3M, { fromRatio: true }) },
    { label: dict.labelPct1M, value: formatPercent(detail.tradingStats.pctChange1M, { fromRatio: true }) },
    { label: dict.labelPct2W, value: formatPercent(detail.tradingStats.pctChange2W, { fromRatio: true }) },
    { label: dict.labelPct1W, value: formatPercent(detail.tradingStats.pctChange1W, { fromRatio: true }) },
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
    },
    {
      label: dict.labelNetIncomeYoyPrev1,
      value: formatPercent(detail.financialStats.netIncomeYoyPrev1, { fromRatio: true }),
    },
    {
      label: dict.labelNetIncomeYoyPrev2,
      value: formatPercent(detail.financialStats.netIncomeYoyPrev2, { fromRatio: true }),
    },
    {
      label: dict.labelNetIncomeQoqLatest,
      value: formatPercent(detail.financialStats.netIncomeQoqLatest, { fromRatio: true }),
    },
    {
      label: dict.labelRevenueYoyLatest,
      value: formatPercent(detail.financialStats.revenueYoyLatest, { fromRatio: true }),
    },
    {
      label: dict.labelRevenueQoqLatest,
      value: formatPercent(detail.financialStats.revenueQoqLatest, { fromRatio: true }),
    },
    {
      label: dict.labelRoeYoyLatest,
      value: formatPercent(detail.financialStats.roeYoyLatest, { fromRatio: true }),
    },
    {
      label: dict.labelRoeQoqLatest,
      value: formatPercent(detail.financialStats.roeQoqLatest, { fromRatio: true }),
    },
  ]);

  candlestickData = limitCandlestickRange(detail.dailyTradeHistory || []);
  renderCandlestickChart();
}

function setStatus(messageKey, isError = false) {
  const dict = translations[currentLang];
  elements.status.textContent = dict[messageKey] || messageKey;
  elements.status.classList.toggle("detail-status--error", isError);
  elements.status.classList.remove("hidden");
  elements.hero.classList.add("hidden");
  elements.grid.classList.add("hidden");
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
    renderDetail(data);
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

document.addEventListener("DOMContentLoaded", initialize);




