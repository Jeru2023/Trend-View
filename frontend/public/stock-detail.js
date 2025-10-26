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
  canvas: document.getElementById("candlestick-canvas"),
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

  candlestickData = detail.dailyTradeHistory || [];
  drawCandlestickChart();
}

function setStatus(messageKey, isError = false) {
  const dict = translations[currentLang];
  elements.status.textContent = dict[messageKey] || messageKey;
  elements.status.classList.toggle("detail-status--error", isError);
  elements.status.classList.remove("hidden");
  elements.hero.classList.add("hidden");
  elements.grid.classList.add("hidden");
}

function showDetail() {
  elements.status.classList.add("hidden");
  elements.grid.classList.remove("hidden");
  elements.hero.classList.remove("hidden");
}

function drawCandlestickChart() {
  const canvas = elements.canvas;
  if (!canvas) {
    return;
  }
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return;
  }

  const devicePixelRatio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth * devicePixelRatio;
  const height = canvas.clientHeight * devicePixelRatio;
  canvas.width = width;
  canvas.height = height;

  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#f9fafb";
  ctx.fillRect(0, 0, width, height);

  const noDataMessage = translations[currentLang].noCandlestickData || "No candlestick data";

  if (!candlestickData.length) {
    ctx.fillStyle = "#9ca3af";
    ctx.font = `${16 * devicePixelRatio}px sans-serif`;
    ctx.textAlign = "center";
    ctx.fillText(noDataMessage, width / 2, height / 2);
    return;
  }

  const highs = candlestickData.map((d) => d.high).filter((v) => v !== null && v !== undefined);
  const lows = candlestickData.map((d) => d.low).filter((v) => v !== null && v !== undefined);
  if (!highs.length || !lows.length) {
    ctx.fillStyle = "#9ca3af";
    ctx.font = `${16 * devicePixelRatio}px sans-serif`;
    ctx.textAlign = "center";
    ctx.fillText(noDataMessage, width / 2, height / 2);
    return;
  }

  const maxPrice = Math.max(...highs);
  const minPrice = Math.min(...lows);
  const padding = (maxPrice - minPrice || 1) * 0.1;
  const chartMax = maxPrice + padding;
  const chartMin = minPrice - padding;

  const chartWidth = width * 0.9;
  const chartHeight = height * 0.8;
  const chartX = width * 0.05;
  const chartY = height * 0.1;

  const candleWidth = Math.max(chartWidth / candlestickData.length * 0.6, 4);
  const candleGap = chartWidth / candlestickData.length;

  const scaleY = (value) =>
    chartY + (chartMax - value) / (chartMax - chartMin || 1) * chartHeight;

  ctx.strokeStyle = "#e5e7eb";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(chartX, chartY);
  ctx.lineTo(chartX, chartY + chartHeight);
  ctx.lineTo(chartX + chartWidth, chartY + chartHeight);
  ctx.stroke();

  candlestickData.forEach((bar, index) => {
    const open = bar.open;
    const close = bar.close;
    const high = bar.high;
    const low = bar.low;
    if ([open, close, high, low].some((v) => v === null || v === undefined)) {
      return;
    }
    const x = chartX + index * candleGap + candleGap / 2;
    const wickTop = scaleY(high);
    const wickBottom = scaleY(low);
    const bodyTop = scaleY(Math.max(open, close));
    const bodyBottom = scaleY(Math.min(open, close));

    const isUp = close >= open;
    const bodyColor = isUp ? "#16a34a" : "#dc2626";

    ctx.strokeStyle = bodyColor;
    ctx.beginPath();
    ctx.moveTo(x, wickTop);
    ctx.lineTo(x, wickBottom);
    ctx.stroke();

    ctx.fillStyle = bodyColor;
    const bodyHeight = Math.max(bodyBottom - bodyTop, 1);
    ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
  });
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
  drawCandlestickChart();
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
  window.addEventListener("resize", () => drawCandlestickChart());
}

document.addEventListener("DOMContentLoaded", initialize);


