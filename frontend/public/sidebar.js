const SIDEBAR_VERSION = "20270509";
const SIDEBAR_SCROLL_KEY = "trend-view-sidebar-scroll";

function getStoredScroll() {
  try {
    const stored = window.localStorage.getItem(SIDEBAR_SCROLL_KEY);
    if (stored === null) {
      return 0;
    }
    const value = Number(stored);
    return Number.isFinite(value) && value >= 0 ? value : 0;
  } catch (error) {
    return 0;
  }
}

function persistScroll(value) {
  try {
    window.localStorage.setItem(SIDEBAR_SCROLL_KEY, String(value));
  } catch (error) {
    /* no-op */
  }
}

function restoreScrollPosition(sidebarRoot) {
  const stored = getStoredScroll();
  if (stored < 0) {
    return;
  }
  sidebarRoot.scrollTop = stored;
  requestAnimationFrame(() => {
    sidebarRoot.scrollTop = stored;
  });
}

function highlightActiveNav(root) {
  if (!root) {
    return null;
  }
  const activeKey = document.body.getAttribute("data-active-nav");
  if (!activeKey) {
    return null;
  }
  const activeItem = root.querySelector(`[data-nav-key="${activeKey}"]`);
  if (activeItem) {
    activeItem.classList.add("nav__item--active");
  }
  return activeItem || null;
}

function ensureSidebarTranslations() {
  if (typeof window.applyTranslations === "function") {
    window.applyTranslations();
  } else {
    window.__SIDEBAR_TRANSLATE_PENDING = true;
  }
}

(function loadSidebar() {
  const container = document.querySelector("[data-sidebar-container]");
  if (!container) {
    return;
  }

  fetch(`sidebar.html?v=${SIDEBAR_VERSION}`)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`Failed to load sidebar (status ${response.status})`);
      }
      return response.text();
    })
    .then((html) => {
      container.innerHTML = html;
      const sidebarRoot = container.querySelector(".sidebar");
      if (sidebarRoot) {
        restoreScrollPosition(sidebarRoot);
        const persistHandler = () => persistScroll(sidebarRoot.scrollTop);
        sidebarRoot.addEventListener("scroll", persistHandler, { passive: true });
        sidebarRoot.addEventListener("click", persistHandler);
        window.addEventListener("beforeunload", persistHandler);
      }
      const activeItem = highlightActiveNav(sidebarRoot);
      enableNavSectionToggle(sidebarRoot, activeItem);
      ensureSidebarTranslations();
    })
    .catch((error) => {
      console.error("Sidebar injection failed:", error);
    });
})();

window.addEventListener("DOMContentLoaded", () => {
  if (window.__SIDEBAR_TRANSLATE_PENDING && typeof window.applyTranslations === "function") {
    window.applyTranslations();
    window.__SIDEBAR_TRANSLATE_PENDING = false;
  }
});

function enableNavSectionToggle(root, activeItem) {
  if (!root) {
    return;
  }

  const STORAGE_KEY = "trend-view-sidebar-section-state";

  const state = (() => {
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (!raw) {
        return {};
      }
      const parsed = JSON.parse(raw);
      return typeof parsed === "object" && parsed ? parsed : {};
    } catch (error) {
      return {};
    }
  })();

  const persist = () => {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (error) {
      /* no-op */
    }
  };

  const sections = Array.from(root.querySelectorAll(".nav-section[data-section]"));
  sections.forEach((section) => {
    const key = section.dataset.section;
    if (!key) {
      return;
    }
    const toggle = section.querySelector("[data-section-toggle]");
    if (!toggle) {
      return;
    }

    const expanded = state[key];
    const shouldCollapse = expanded === false;
    if (shouldCollapse) {
      section.classList.add("nav-section--collapsed");
    }
    toggle.setAttribute("aria-expanded", shouldCollapse ? "false" : "true");

    toggle.addEventListener("click", () => {
      const collapsed = section.classList.toggle("nav-section--collapsed");
      toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
      state[key] = !collapsed;
      persist();
    });
  });

  if (activeItem) {
    const parentSection = activeItem.closest(".nav-section[data-section]");
    if (parentSection) {
      const key = parentSection.dataset.section;
      if (key && parentSection.classList.contains("nav-section--collapsed")) {
        parentSection.classList.remove("nav-section--collapsed");
        const toggle = parentSection.querySelector("[data-section-toggle]");
        if (toggle) {
          toggle.setAttribute("aria-expanded", "true");
        }
        state[key] = true;
        persist();
      }
    }
  }
}
