const params = new URLSearchParams(window.location.search);
const storageKey = params.get("key");

function loadSnapshotPayload() {
  if (!storageKey) {
    return [];
  }
  try {
    const raw = sessionStorage.getItem(storageKey);
    sessionStorage.removeItem(storageKey);
    return raw ? JSON.parse(raw) : [];
  } catch (error) {
    console.error("Failed to parse snapshot payload", error);
    return [];
  }
}

function normalizeSinaCode(code) {
  if (!code) {
    return null;
  }
  const upper = code.toUpperCase();
  const match = upper.match(/^(\d{6})(?:\.(SH|SZ|BJ))?$/);
  if (match) {
    const digits = match[1];
    const suffix = match[2] || "";
    if (suffix === "SH") {
      return `sh${digits}`;
    }
    if (suffix === "SZ") {
      return `sz${digits}`;
    }
    if (suffix === "BJ") {
      return `bj${digits}`;
    }
    const first = digits[0];
    if (["5", "6", "9"].includes(first)) {
      return `sh${digits}`;
    }
    if (["0", "2", "3"].includes(first)) {
      return `sz${digits}`;
    }
    if (["4", "8"].includes(first)) {
      return `bj${digits}`;
    }
  }
  return null;
}

function buildSnapshotCard(entry) {
  const container = document.createElement("article");
  container.className = "snapshot-card";
  const header = document.createElement("div");
  header.style.display = "flex";
  header.style.alignItems = "center";
  header.style.gap = "8px";
  const titleLink = document.createElement("a");
  const normalizedDetailCode = entry.code ? entry.code : "";
  const detailUrl = normalizedDetailCode
    ? `stock-detail.html?code=${encodeURIComponent(normalizedDetailCode)}`
    : "#";
  titleLink.href = detailUrl;
  titleLink.target = "_blank";
  titleLink.rel = "noopener";
  titleLink.textContent = entry.name || entry.code || "--";
  titleLink.style.fontSize = "1rem";
  titleLink.style.fontWeight = "600";
  titleLink.style.textDecoration = "none";
  titleLink.style.color = "#0f172a";
  const codeSpan = document.createElement("span");
  codeSpan.textContent = entry.code || "--";
  codeSpan.style.color = "#475569";
  codeSpan.style.fontSize = "0.9rem";
  header.appendChild(titleLink);
  header.appendChild(codeSpan);

  const image = document.createElement("img");
  const normalized = normalizeSinaCode(entry.code);
  if (normalized) {
    image.src = `https://image.sinajs.cn/newchart/daily/n/${normalized}.gif`;
    image.alt = `${entry.code} snapshot`;
  } else {
    image.alt = "Snapshot unavailable";
  }

  container.appendChild(header);
  container.appendChild(image);
  return container;
}

function renderSnapshots(entries) {
  const grid = document.getElementById("snapshot-container");
  const emptyState = document.getElementById("snapshot-empty");
  const meta = document.getElementById("snapshot-meta");
  if (!entries.length) {
    grid.classList.add("hidden");
    emptyState.classList.remove("hidden");
    meta.textContent = "未能获取快照数据，请返回列表重试。";
    return;
  }
  grid.classList.remove("hidden");
  emptyState.classList.add("hidden");
  meta.textContent = `共 ${entries.length} 个股票`;
  const fragment = document.createDocumentFragment();
  entries.forEach((entry) => {
    fragment.appendChild(buildSnapshotCard(entry));
  });
  grid.innerHTML = "";
  grid.appendChild(fragment);
}

const payload = loadSnapshotPayload();
renderSnapshots(payload);
