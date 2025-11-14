console.info("Investment Journal bundle v20261101");

const translations = getTranslations("investmentJournal");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  todayButton: document.getElementById("investment-journal-today"),
  monthLabel: document.getElementById("investment-journal-month"),
  weekdays: document.getElementById("investment-journal-weekdays"),
  calendarGrid: document.getElementById("investment-journal-calendar"),
  prevMonth: document.getElementById("investment-journal-prev"),
  nextMonth: document.getElementById("investment-journal-next"),
  selectedLabel: document.getElementById("investment-journal-selected"),
  reviewEditor: document.getElementById("investment-journal-review"),
  planEditor: document.getElementById("investment-journal-plan"),
  toolbar: document.querySelector(".investment-journal__toolbar"),
  saveButton: document.getElementById("investment-journal-save"),
  status: document.getElementById("investment-journal-status"),
};

const notesElements = {
  table: document.getElementById("investment-journal-notes"),
  body: document.querySelector("#investment-journal-notes tbody"),
  range: document.getElementById("investment-journal-notes-range"),
};

const today = new Date();
const journalState = {
  visibleMonth: startOfMonth(today),
  selectedDate: startOfDay(today),
  entriesByDate: new Map(),
  isDirty: false,
  isSaving: false,
  isLoading: false,
  activeEditor: null,
  currentEntry: null,
};
const notesState = {
  rangeStart: startOfDay(new Date(today.getFullYear(), today.getMonth() - 3, today.getDate())),
  rangeEnd: startOfDay(today),
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
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  return "zh";
}

let currentLang = getInitialLanguage();

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

function startOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function startOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function endOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth() + 1, 0);
}

function toISODate(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function isSameDay(a, b) {
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

function isSameMonth(a, b) {
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth();
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.title = dict.title || "Investment Journal";
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
  setEditorPlaceholder(elements.reviewEditor, dict.journalReviewPlaceholder || "Summarize today…");
  setEditorPlaceholder(elements.planEditor, dict.journalPlanPlaceholder || "Plan tomorrow…");
  renderWeekdays();
  updateMonthLabel();
  updateSelectedDateLabel();
  if (!journalState.isDirty && !journalState.isSaving && !journalState.isLoading) {
    const entry = journalState.entriesByDate.get(toISODate(journalState.selectedDate));
    if (entry && entry.updatedAt) {
      setStatus(`${dict.journalLastSaved || "Saved at"} ${formatDateTime(entry.updatedAt)}`, "success");
    } else {
      setStatus(dict.journalEmpty || "Select a date on the calendar to start writing.", "info");
    }
  }
  renderNotesRange();
  if (!notesState.entries.length) {
    const empty = getDict().notesEmpty || "No stock notes in this range.";
    setNotesEmpty(empty);
  }
  updateSaveButton();
}

function setEditorPlaceholder(node, placeholder) {
  if (!node) return;
  node.dataset.placeholder = placeholder || "";
}

function renderWeekdays() {
  if (!elements.weekdays) return;
  const dict = getDict();
  const labels = (dict.journalWeekdaysShort || "Sun,Mon,Tue,Wed,Thu,Fri,Sat").split(",").slice(0, 7);
  elements.weekdays.innerHTML = labels.map((label) => `<span>${label.trim()}</span>`).join("");
}

function updateMonthLabel() {
  if (!elements.monthLabel) return;
  const formatter = new Intl.DateTimeFormat(currentLang === "zh" ? "zh-CN" : "en-US", {
    year: "numeric",
    month: "long",
  });
  elements.monthLabel.textContent = formatter.format(journalState.visibleMonth);
}

function updateSelectedDateLabel() {
  if (!elements.selectedLabel) return;
  elements.selectedLabel.textContent = formatSelectedDate(journalState.selectedDate);
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const lang = btn.dataset.lang;
      if (lang && lang !== currentLang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
        loadJournalMonth(journalState.visibleMonth, { selectDate: journalState.selectedDate, skipStatus: true });
      }
    });
  });

  elements.prevMonth?.addEventListener("click", () => changeMonth(-1));
  elements.nextMonth?.addEventListener("click", () => changeMonth(1));
  elements.todayButton?.addEventListener("click", () => selectDate(new Date()));

  [elements.reviewEditor, elements.planEditor].forEach((editor) => {
    if (!editor) return;
    editor.addEventListener("focus", () => {
      journalState.activeEditor = editor;
    });
    editor.addEventListener("input", () => {
      journalState.isDirty = true;
      setStatus(getDict().journalStatusIdle || "Draft not saved.", "warn");
      updateSaveButton();
    });
  });

  elements.saveButton?.addEventListener("click", () => saveEntry());

  if (elements.toolbar) {
    elements.toolbar.querySelectorAll("button[data-command]").forEach((btn) => {
      btn.addEventListener("click", (event) => {
        event.preventDefault();
        applyEditorCommand(btn.dataset.command);
      });
    });
  }
}

function changeMonth(offset) {
  const target = new Date(journalState.visibleMonth.getFullYear(), journalState.visibleMonth.getMonth() + offset, 1);
  loadJournalMonth(target, { selectDate: journalState.selectedDate });
}

async function loadJournalMonth(targetMonth, options = {}) {
  journalState.visibleMonth = startOfMonth(targetMonth);
  updateMonthLabel();
  const dict = getDict();
  if (!options.skipStatus) {
    setStatus(dict.journalStatusLoading || "Loading journal…", "info");
  }
  journalState.isLoading = true;
  try {
    const startStr = toISODate(journalState.visibleMonth);
    const endStr = toISODate(endOfMonth(journalState.visibleMonth));
    const url = new URL(`${API_BASE}/journal/entries`);
    url.searchParams.set("startDate", startStr);
    url.searchParams.set("endDate", endStr);
    const response = await fetch(url.toString(), { cache: "no-store" });
    if (!response.ok) {
      throw new Error(response.statusText || `HTTP ${response.status}`);
    }
    const data = await response.json();
    journalState.entriesByDate = new Map();
    if (Array.isArray(data)) {
      data.forEach((entry) => {
        if (entry?.entryDate) {
          journalState.entriesByDate.set(entry.entryDate, entry);
        }
      });
    }
    renderCalendar();
    if (options.selectDate) {
      selectDate(options.selectDate, { skipMonthReload: true });
    } else if (isSameMonth(journalState.selectedDate, journalState.visibleMonth)) {
      selectDate(journalState.selectedDate, { skipMonthReload: true });
    } else {
      selectDate(startOfDay(journalState.visibleMonth), { skipMonthReload: true });
    }
  } catch (error) {
    console.error("Failed to load investment journal", error);
    setStatus(error?.message || dict.journalStatusError || "Failed to load journal entry.", "error");
    journalState.entriesByDate = new Map();
    renderCalendar();
  } finally {
    journalState.isLoading = false;
    updateSaveButton();
  }
}

function renderCalendar() {
  if (!elements.calendarGrid) return;
  const fragment = document.createDocumentFragment();
  const gridStart = new Date(journalState.visibleMonth);
  gridStart.setDate(1 - gridStart.getDay());
  for (let i = 0; i < 42; i += 1) {
    const current = new Date(gridStart);
    current.setDate(gridStart.getDate() + i);
    const iso = toISODate(current);
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.date = iso;
    button.textContent = current.getDate();
    if (!isSameMonth(current, journalState.visibleMonth)) {
      button.classList.add("is-outside");
    }
    if (isSameDay(current, today)) {
      button.classList.add("is-today");
    }
    if (isSameDay(current, journalState.selectedDate)) {
      button.classList.add("is-selected");
    }
    const entry = journalState.entriesByDate.get(iso);
    if (entry && entryHasContent(entry)) {
      button.classList.add("has-entry");
    }
    button.addEventListener("click", () => selectDate(current));
    fragment.appendChild(button);
  }
  elements.calendarGrid.innerHTML = "";
  elements.calendarGrid.appendChild(fragment);
}

function entryHasContent(entry) {
  if (!entry) return false;
  const review = (entry.reviewHtml || "").replace(/<br\s*\/?>/gi, "").trim();
  const plan = (entry.planHtml || "").replace(/<br\s*\/?>/gi, "").trim();
  return Boolean(review || plan);
}

function selectDate(date, options = {}) {
  const normalized = startOfDay(date);
  if (!options.skipMonthReload && !isSameMonth(normalized, journalState.visibleMonth)) {
    loadJournalMonth(normalized, { selectDate: normalized });
    return;
  }
  journalState.selectedDate = normalized;
  const iso = toISODate(normalized);
  const entry = journalState.entriesByDate.get(iso) || null;
  journalState.currentEntry = entry;
  journalState.isDirty = false;
  setEditorHtml(elements.reviewEditor, entry?.reviewHtml || "");
  setEditorHtml(elements.planEditor, entry?.planHtml || "");
  renderCalendar();
  updateSelectedDateLabel();
  const dict = getDict();
  if (entry && entry.updatedAt) {
    setStatus(`${dict.journalLastSaved || "Saved at"} ${formatDateTime(entry.updatedAt)}`, "success");
  } else {
    setStatus(dict.journalEmpty || "Select a date on the calendar to start writing.", "info");
  }
  updateSaveButton();
}

function setEditorHtml(editor, html) {
  if (!editor) return;
  editor.innerHTML = html || "";
}

function getEditorHtml(editor) {
  if (!editor) return "";
  return editor.innerHTML.trim();
}

function sanitizeHtml(html) {
  if (!html) return "";
  const temp = document.createElement("div");
  temp.innerHTML = html;
  temp.querySelectorAll("script,style").forEach((node) => node.remove());
  temp.querySelectorAll("*").forEach((node) => {
    [...node.attributes].forEach((attr) => {
      if (attr.name.toLowerCase().startsWith("on")) {
        node.removeAttribute(attr.name);
      }
      if (typeof attr.value === "string" && attr.value.toLowerCase().startsWith("javascript:")) {
        node.setAttribute(attr.name, attr.value.replace(/javascript:/gi, ""));
      }
    });
  });
  return temp.innerHTML.trim();
}

async function saveEntry() {
  if (journalState.isSaving) return;
  const dict = getDict();
  journalState.isSaving = true;
  updateSaveButton();
  setStatus(dict.journalStatusSaving || "Saving entry…", "info");
  const iso = toISODate(journalState.selectedDate);
  const payload = {
    reviewHtml: sanitizeHtml(getEditorHtml(elements.reviewEditor)),
    planHtml: sanitizeHtml(getEditorHtml(elements.planEditor)),
  };
  try {
    const response = await fetch(`${API_BASE}/journal/entries/${iso}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(response.statusText || `HTTP ${response.status}`);
    }
    const data = await response.json();
    if (data?.entryDate) {
      journalState.entriesByDate.set(data.entryDate, data);
    }
    journalState.isDirty = false;
    journalState.currentEntry = data;
    setStatus(`${dict.journalLastSaved || "Saved at"} ${formatDateTime(data?.updatedAt)}`, "success");
    renderCalendar();
  } catch (error) {
    console.error("Failed to save journal entry", error);
    setStatus(error?.message || dict.journalStatusError || "Failed to save journal entry.", "error");
  } finally {
    journalState.isSaving = false;
    updateSaveButton();
  }
}

function setStatus(message, tone = "info") {
  if (!elements.status) return;
  elements.status.textContent = message || "";
  if (message) {
    elements.status.dataset.tone = tone;
  } else {
    elements.status.removeAttribute("data-tone");
  }
}

function updateSaveButton() {
  if (!elements.saveButton) return;
  const dict = getDict();
  elements.saveButton.disabled = journalState.isSaving;
  elements.saveButton.textContent = journalState.isSaving
    ? dict.journalSaving || "Saving…"
    : dict.journalSave || "Save Entry";
}

function formatDateTime(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  const formatter = new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  return formatter.format(date);
}

function formatSelectedDate(date) {
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
  }).format(date);
}

function applyEditorCommand(command) {
  const target = journalState.activeEditor || elements.reviewEditor;
  if (!target || !command) return;
  target.focus();
  document.execCommand(command, false, null);
}

function renderNotesRange() {
  if (!notesElements.range) return;
  const formatter = new Intl.DateTimeFormat(currentLang === "zh" ? "zh-CN" : "en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  notesElements.range.textContent = `${formatter.format(notesState.rangeStart)} — ${formatter.format(notesState.rangeEnd)}`;
}

function setNotesEmpty(message) {
  if (!notesElements.body) return;
  notesElements.body.innerHTML = "";
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 3;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  notesElements.body.appendChild(row);
}

async function loadRecentNotes() {
  if (!notesElements.table) return;
  const dict = getDict();
  renderNotesRange();
  try {
    const startStr = toISODate(notesState.rangeStart);
    const endStr = toISODate(notesState.rangeEnd);
    const url = new URL(`${API_BASE}/journal/stock-notes`);
    url.searchParams.set("startDate", startStr);
    url.searchParams.set("endDate", endStr);
    url.searchParams.set("limit", "200");
    const response = await fetch(url.toString(), { cache: "no-store" });
    if (!response.ok) {
      throw new Error(response.statusText || `HTTP ${response.status}`);
    }
    const data = await response.json();
    notesState.entries = Array.isArray(data?.items) ? data.items : [];
    renderNotesTable();
  } catch (error) {
    console.error("Failed to load stock notes", error);
    setNotesEmpty(error?.message || dict.notesEmpty || "No stock notes in this range.");
  }
}

function renderNotesTable() {
  if (!notesElements.body) return;
  const dict = getDict();
  const entries = Array.isArray(notesState.entries) ? notesState.entries : [];
  if (!entries.length) {
    setNotesEmpty(dict.notesEmpty || "No stock notes in this range.");
    return;
  }
  const fragment = document.createDocumentFragment();
  entries.forEach((entry) => {
    const row = document.createElement("tr");
    const dateCell = document.createElement("td");
    dateCell.textContent = formatDateTime(entry.updatedAt || entry.createdAt);
    const codeCell = document.createElement("td");
    const stockCode = entry.stockCode || entry.stock_code || "";
    const stockName = entry.stockName || entry.stock_name || "";
    if (stockCode) {
      const link = document.createElement("a");
      link.href = `stock-detail.html?code=${encodeURIComponent(stockCode)}`;
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = stockName ? `${stockName} (${stockCode})` : stockCode;
      codeCell.appendChild(link);
    } else {
      codeCell.textContent = stockName || "--";
    }
    const contentCell = document.createElement("td");
    contentCell.textContent = entry.content || "--";
    row.appendChild(dateCell);
    row.appendChild(codeCell);
    row.appendChild(contentCell);
    fragment.appendChild(row);
  });
  notesElements.body.innerHTML = "";
  notesElements.body.appendChild(fragment);
}

function initialize() {
  persistLanguage(currentLang);
  applyTranslations();
  bindEvents();
  loadJournalMonth(journalState.visibleMonth, { selectDate: journalState.selectedDate });
  loadRecentNotes();
}

initialize();

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
