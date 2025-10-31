const translations = getTranslations("fedStatements");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
let latestItems = [];
let refreshTimer = null;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  list: document.getElementById("fed-statements-list"),
  lastSynced: document.getElementById("fed-statements-last-synced"),
  refreshButton: document.getElementById("fed-statements-refresh"),
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
  const htmlLang = document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) {
    return htmlLang;
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

function getDict() {
  return translations[currentLang] || translations.en;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return date.toLocaleDateString(locale, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  } catch (error) {
    return String(value);
  }
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    })}`;
  } catch (error) {
    return String(value);
  }
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = dict.title || document.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      if (key === "refreshButton" && elements.refreshButton?.dataset.loading === "1") {
        el.textContent = dict.refreshing || value;
      } else {
        el.textContent = value;
      }
    }
  });

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  renderStatements(latestItems);
}

function setLang(lang) {
  if (!translations[lang] || lang === currentLang) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  applyTranslations();
}

function setRefreshLoading(loading) {
  if (!elements.refreshButton) {
    return;
  }
  elements.refreshButton.disabled = loading;
  if (loading) {
    elements.refreshButton.dataset.loading = "1";
    const dict = getDict();
    elements.refreshButton.textContent = dict.refreshing || "Refreshing";
  } else {
    delete elements.refreshButton.dataset.loading;
    elements.refreshButton.textContent = getDict().refreshButton || "Refresh";
  }
}

function clearList() {
  if (elements.list) {
    elements.list.innerHTML = "";
  }
}

function renderEmpty() {
  if (!elements.list) {
    return;
  }
  const message =
    elements.list.dataset[`empty${currentLang.toUpperCase()}`] || getDict().empty || "No data.";
  const empty = document.createElement("div");
  empty.className = "statement-empty";
  empty.textContent = message;
  clearList();
  elements.list.appendChild(empty);
}

function renderStatements(items = []) {
  if (!elements.list) {
    return;
  }
  if (!items.length) {
    renderEmpty();
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = "statement-card";

    const header = document.createElement("div");
    header.className = "statement-card__header";

    const title = document.createElement("h3");
    title.className = "statement-card__title";
    title.textContent = item.title || getDict().untitled || "Untitled";

    const meta = document.createElement("div");
    meta.className = "statement-card__meta";
    const published = item.statement_date || item.statementDate;
    meta.textContent = formatDate(published);

    header.appendChild(title);
    header.appendChild(meta);
    card.appendChild(header);

    if (item.content) {
      const content = document.createElement("div");
      content.className = "statement-card__content";
      item.content
        .split(/\n+/)
        .map((paragraph) => paragraph.trim())
        .filter(Boolean)
        .forEach((paragraph) => {
          const p = document.createElement("p");
          p.textContent = paragraph;
          content.appendChild(p);
        });
      card.appendChild(content);
    }

    if (item.url) {
      const link = document.createElement("a");
      link.className = "statement-card__link";
      link.href = item.url;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = getDict().viewOriginal || "View original";
      card.appendChild(link);
    }

    if (item.updated_at || item.updatedAt) {
      const footer = document.createElement("div");
      footer.className = "statement-card__footer";
      footer.textContent = `${getDict().updatedAtLabel || "Updated"}: ${formatDateTime(
        item.updated_at || item.updatedAt
      )}`;
      card.appendChild(footer);
    }

    fragment.appendChild(card);
  });

  clearList();
  elements.list.appendChild(fragment);
}

function updateLastSynced(value) {
  if (!elements.lastSynced) {
    return;
  }
  elements.lastSynced.textContent = value ? formatDateTime(value) : "--";
}

async function loadStatements() {
  try {
    const response = await fetch(`${API_BASE}/macro/fed-statements?limit=20`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderStatements(latestItems);
    updateLastSynced(data.lastSyncedAt);
  } catch (error) {
    console.error("Failed to load Fed statements", error);
    renderEmpty();
    updateLastSynced(null);
  }
}

async function triggerRefresh() {
  if (elements.refreshButton?.disabled) {
    return;
  }
  setRefreshLoading(true);
  try {
    const response = await fetch(`${API_BASE}/control/sync/fed-statements`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ limit: 5 }),
    });
    if (!response.ok) {
      throw new Error(`Refresh failed with status ${response.status}`);
    }
  } catch (error) {
    console.error("Failed to trigger refresh", error);
    setRefreshLoading(false);
    return;
  }

  if (refreshTimer) {
    clearTimeout(refreshTimer);
  }
  refreshTimer = setTimeout(() => {
    loadStatements().finally(() => {
      setRefreshLoading(false);
      refreshTimer = null;
    });
  }, 1500);
}

function initLanguageSwitch() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLang(btn.dataset.lang))
  );
}

function initActions() {
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", triggerRefresh);
  }
}

initLanguageSwitch();
initActions();
applyTranslations();
loadStatements();


window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
