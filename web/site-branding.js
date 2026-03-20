(function () {
  "use strict";

  const fallbackConfig = Object.freeze({
    site_name: "GameDen.gg",
    site_url: "https://gameden.gg",
    site_description: "Discover game deals, analytics, player trends, and price history on GameDen.gg.",
    // Explicit API base for production static deployments.
    api_base: "https://gameden.onrender.com",
  });
  const ABSOLUTE_URL_RE = /^(https?:)?\/\//i;
  const SPECIAL_SCHEME_RE = /^(mailto:|tel:|data:|javascript:)/i;
  const STATIC_PAGE_PATHS = new Set([
    "/",
    "/index.html",
    "/game.html",
    "/history.html",
    "/watchlist.html",
    "/all-results.html",
    "/game-detail.html",
  ]);
  const STATIC_ASSET_EXTENSIONS = [
    ".html",
    ".css",
    ".js",
    ".mjs",
    ".ico",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".avif",
    ".woff",
    ".woff2",
    ".ttf",
    ".map",
    ".json",
    ".xml",
    ".txt",
    ".webmanifest",
    ".manifest",
  ];
  const API_EXACT_PATHS = new Set([
    "/health",
    "/metrics",
    "/search",
    "/alerts",
    "/wishlist",
    "/watchlist",
    "/worth-buying-now",
    "/trending-deals",
    "/historical-lows",
  ]);
  const API_PATH_PREFIXES = [
    "/api/",
    "/dashboard/",
    "/sales/",
    "/games/",
    "/deals/",
    "/leaderboards/",
    "/wishlist/",
    "/deal-watchlists/",
    "/notifications/",
  ];
  const warnedKeys = new Set();
  const NEW_SIGNAL_DEFAULT_CAP = 800;
  const newSignalBuckets = new Map();
  const SKELETON_STYLE_ID = "gameden-skeleton-styles";
  const VIEWER_ID_STORAGE_KEY = "gameden.user_id";
  const VIEWER_ID_HEADER_NAME = "x-gameden-viewer";
  const VIEWER_ID_RE = /^anon_[0-9a-f]{32}$/;
  const LEGACY_VIEWER_IDS = new Set(["legacy-user", "anonymous", "guest"]);

  function warnOnce(key, message) {
    if (warnedKeys.has(key)) {
      return;
    }
    warnedKeys.add(key);
    if (typeof console !== "undefined" && typeof console.warn === "function") {
      console.warn(`[GameDenSite] ${message}`);
    }
  }

  function _newViewerId() {
    if (typeof crypto !== "undefined" && crypto && typeof crypto.randomUUID === "function") {
      return `anon_${String(crypto.randomUUID()).replaceAll("-", "").toLowerCase()}`;
    }
    const fallback = `${Date.now().toString(16)}${Math.floor(Math.random() * 1e12).toString(16)}`.slice(0, 32);
    return `anon_${fallback.padEnd(32, "0")}`;
  }

  function _normalizeViewerId(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (!normalized) return "";
    if (VIEWER_ID_RE.test(normalized)) return normalized;
    return "";
  }

  function _readStoredViewerId() {
    try {
      const stored = String(window.localStorage.getItem(VIEWER_ID_STORAGE_KEY) || "").trim().toLowerCase();
      if (!stored || LEGACY_VIEWER_IDS.has(stored)) return "";
      return _normalizeViewerId(stored);
    } catch (_error) {
      return "";
    }
  }

  function _persistViewerId(value) {
    const normalized = _normalizeViewerId(value);
    if (!normalized) return "";
    try {
      window.localStorage.setItem(VIEWER_ID_STORAGE_KEY, normalized);
    } catch (_error) {
      // Ignore storage failures.
    }
    return normalized;
  }

  function getViewerId() {
    const stored = _readStoredViewerId();
    if (stored) return stored;
    return _persistViewerId(_newViewerId());
  }

  function normalizeSiteUrl(rawValue) {
    const value = String(rawValue || "").trim() || fallbackConfig.site_url;
    try {
      const parsed = new URL(value, window.location.origin);
      const normalizedPath = parsed.pathname.replace(/\/+$/, "");
      return `${parsed.protocol}//${parsed.host}${normalizedPath}`;
    } catch (_error) {
      return fallbackConfig.site_url;
    }
  }

  const runtimeConfig = window.__GAMEDEN_SITE__ || {};
  const runtimeApiBase = Object.prototype.hasOwnProperty.call(runtimeConfig, "api_base")
    ? runtimeConfig.api_base
    : fallbackConfig.api_base;
  const siteConfig = Object.freeze({
    site_name: String(runtimeConfig.site_name || fallbackConfig.site_name).trim() || fallbackConfig.site_name,
    site_url: normalizeSiteUrl(runtimeConfig.site_url || fallbackConfig.site_url),
    site_description: String(runtimeConfig.site_description || fallbackConfig.site_description).trim() || fallbackConfig.site_description,
    api_base: String(runtimeApiBase ?? "").trim(),
  });

  function absoluteUrl(path) {
    const normalizedPath = String(path || "/").startsWith("/") ? String(path || "/") : `/${String(path || "/")}`;
    return `${siteConfig.site_url.replace(/\/+$/, "")}${normalizedPath}`;
  }

  function updateMetaTag(selector, attribute, value) {
    const node = document.querySelector(selector);
    if (!node || typeof value !== "string" || !value) {
      return;
    }
    node.setAttribute(attribute, value);
  }

  function resolveApiUrl(url) {
    const value = String(url || "").trim();
    if (!value || ABSOLUTE_URL_RE.test(value)) {
      return value;
    }

    if (SPECIAL_SCHEME_RE.test(value)) {
      return value;
    }

    const match = value.match(/^([^?#]*)([?#].*)?$/);
    const rawPath = match ? match[1] : value;
    const suffix = match && match[2] ? match[2] : "";

    if (!rawPath || rawPath.startsWith("../")) {
      return value;
    }

    const normalizedPath = (function normalizeRelativePath(path) {
      if (path.startsWith("/")) return path;
      if (path.startsWith("./")) return `/${path.slice(2)}`;
      return `/${path}`;
    })(rawPath).replace(/^\/+/, "/");

    const normalizedLower = normalizedPath.toLowerCase();
    const isStaticPage = STATIC_PAGE_PATHS.has(normalizedLower);
    const isStaticAsset = STATIC_ASSET_EXTENSIONS.some((ext) => normalizedLower.endsWith(ext));
    if (isStaticPage || isStaticAsset) {
      return value;
    }

    const isApiRoute =
      API_EXACT_PATHS.has(normalizedLower) ||
      API_PATH_PREFIXES.some((prefix) => normalizedLower.startsWith(prefix));

    if (!isApiRoute) {
      return value;
    }

    const apiBase = String(siteConfig.api_base || "").trim().replace(/\/+$/, "");
    if (!apiBase) {
      return value;
    }

    return `${apiBase}${normalizedPath}${suffix}`;
  }

  function _parseJsonBody(responseText, url, status, isOk) {
    const body = String(responseText || "");
    if (!body.trim()) {
      return { payload: null, hasBody: false, rawBody: "" };
    }
    try {
      return { payload: JSON.parse(body), hasBody: true, rawBody: body };
    } catch (_error) {
      if (isOk) {
        const parseError = new Error(`Invalid JSON response for ${url}`);
        parseError.status = status;
        parseError.url = url;
        throw parseError;
      }
      return { payload: null, hasBody: true, rawBody: body };
    }
  }

  function _extractErrorDetail(payload, rawBody, fallbackMessage) {
    if (payload && typeof payload === "object") {
      return payload.detail || payload.error || payload.message || fallbackMessage;
    }
    if (typeof rawBody === "string" && rawBody.trim()) {
      return rawBody.trim();
    }
    return fallbackMessage;
  }

  async function _requestJson(url, requestUrl, options = {}) {
    const requestOptions = { ...options };
    const headers = new Headers(requestOptions.headers || {});
    const viewerId = getViewerId();
    if (viewerId && !headers.has(VIEWER_ID_HEADER_NAME)) {
      headers.set(VIEWER_ID_HEADER_NAME, viewerId);
    }
    requestOptions.headers = headers;
    if (requestOptions.credentials === undefined) {
      requestOptions.credentials = "include";
    }

    let response;
    try {
      response = await fetch(requestUrl, requestOptions);
    } catch (networkError) {
      const error = new Error(`Network request failed for ${url} (${requestUrl})`);
      error.url = url;
      error.requestUrl = requestUrl;
      error.cause = networkError;
      throw error;
    }

    const responseViewerId = _normalizeViewerId(response.headers.get("x-gameden-viewer"));
    if (responseViewerId) {
      _persistViewerId(responseViewerId);
    }

    if (response.status === 204) return null;

    const rawBody = await response.text();
    const parsed = _parseJsonBody(rawBody, url, response.status, response.ok);

    if (!response.ok) {
      const fallbackMessage = `Failed to load ${url} (${requestUrl}): ${response.status}`;
      const error = new Error(_extractErrorDetail(parsed.payload, parsed.rawBody, fallbackMessage));
      error.status = response.status;
      error.url = url;
      error.requestUrl = requestUrl;
      throw error;
    }

    return parsed.hasBody ? parsed.payload : null;
  }

  async function fetchJson(url, options = {}) {
    const requestUrl = resolveApiUrl(url);
    return _requestJson(url, requestUrl, options);
  }

  function applyMetadata(meta) {
    const metadata = meta || {};
    const path = metadata.path || "/";
    const canonicalUrl = absoluteUrl(path);

    const pageTitle = metadata.title || document.title || siteConfig.site_name;
    const pageDescription = metadata.description || siteConfig.site_description;
    const ogTitle = metadata.ogTitle || pageTitle;
    const ogDescription = metadata.ogDescription || pageDescription;
    const twitterTitle = metadata.twitterTitle || ogTitle;
    const twitterDescription = metadata.twitterDescription || ogDescription;
    const ogImage = String(metadata.ogImage || metadata.image || "").trim();
    const twitterImage = String(metadata.twitterImage || ogImage || metadata.image || "").trim();
    const ogImageAlt = String(metadata.ogImageAlt || metadata.imageAlt || "").trim();

    if (pageTitle) {
      document.title = pageTitle;
    }

    updateMetaTag("meta[name='description']", "content", pageDescription);
    updateMetaTag("meta[name='application-name']", "content", siteConfig.site_name);
    updateMetaTag("meta[property='og:site_name']", "content", siteConfig.site_name);
    updateMetaTag("meta[property='og:title']", "content", ogTitle);
    updateMetaTag("meta[property='og:description']", "content", ogDescription);
    updateMetaTag("meta[property='og:url']", "content", canonicalUrl);
    updateMetaTag("meta[name='twitter:title']", "content", twitterTitle);
    updateMetaTag("meta[name='twitter:description']", "content", twitterDescription);
    updateMetaTag("meta[name='twitter:url']", "content", canonicalUrl);
    if (ogImage) {
      updateMetaTag("meta[property='og:image']", "content", ogImage);
    }
    if (ogImageAlt) {
      updateMetaTag("meta[property='og:image:alt']", "content", ogImageAlt);
    }
    if (twitterImage) {
      updateMetaTag("meta[name='twitter:image']", "content", twitterImage);
    }
    updateMetaTag("link[rel='canonical']", "href", canonicalUrl);
  }

  function _toFiniteNumber(value) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return null;
    return parsed;
  }

  function getDealConfidence(payload) {
    const source = payload && typeof payload === "object" ? payload : {};
    const scoreCandidates = [
      source.buy_score,
      source.worth_buying_score,
      source.deal_score,
      source.score,
    ];

    let rawScore = null;
    for (const candidate of scoreCandidates) {
      const parsed = _toFiniteNumber(candidate);
      if (parsed === null) continue;
      rawScore = parsed;
      break;
    }
    if (rawScore === null) {
      return null;
    }

    const score = Math.max(0, Math.min(100, rawScore));
    let confidenceLabel = "Wait";
    let confidenceColor = "#9eb8e7";
    let confidenceIcon = "WT";
    let className = "wait";

    if (score >= 85) {
      confidenceLabel = "Strong Buy";
      confidenceColor = "#5ce4a9";
      confidenceIcon = "SB";
      className = "strong-buy";
    } else if (score >= 70) {
      confidenceLabel = "Good Deal";
      confidenceColor = "#6fe8ff";
      confidenceIcon = "GD";
      className = "good-deal";
    } else if (score >= 50) {
      confidenceLabel = "Fair Price";
      confidenceColor = "#ffc77a";
      confidenceIcon = "FP";
      className = "fair-price";
    }

    return {
      score: Math.round(score * 10) / 10,
      confidence_label: confidenceLabel,
      confidence_color: confidenceColor,
      confidence_icon: confidenceIcon,
      class_name: className,
    };
  }

  function _normalizeNewSignalToken(value) {
    return String(value ?? "").trim().toLowerCase();
  }

  function markNewSignal(scope, itemKey, options = {}) {
    const scopeToken = _normalizeNewSignalToken(scope);
    const itemToken = _normalizeNewSignalToken(itemKey);
    if (!scopeToken || !itemToken) {
      return false;
    }

    const requestedCap = Number(options.maxEntries);
    const maxEntries = Number.isFinite(requestedCap)
      ? Math.max(50, Math.min(3000, Math.trunc(requestedCap)))
      : NEW_SIGNAL_DEFAULT_CAP;

    let bucket = newSignalBuckets.get(scopeToken);
    if (!bucket || !(bucket.seen instanceof Set) || !Array.isArray(bucket.order)) {
      bucket = {
        seen: new Set(),
        order: [],
        maxEntries,
      };
      newSignalBuckets.set(scopeToken, bucket);
    } else {
      bucket.maxEntries = maxEntries;
    }

    if (bucket.seen.has(itemToken)) {
      return false;
    }

    bucket.seen.add(itemToken);
    bucket.order.push(itemToken);

    while (bucket.order.length > bucket.maxEntries) {
      const oldest = bucket.order.shift();
      if (!oldest) continue;
      bucket.seen.delete(oldest);
    }

    return true;
  }

  function resetNewSignalScope(scope) {
    const scopeToken = _normalizeNewSignalToken(scope);
    if (!scopeToken) return;
    newSignalBuckets.delete(scopeToken);
  }

  function _toClampedInt(value, fallback, min, max) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return fallback;
    const normalized = Math.trunc(parsed);
    return Math.max(min, Math.min(max, normalized));
  }

  function _repeatMarkup(count, mapper) {
    const rows = [];
    for (let index = 0; index < count; index += 1) {
      rows.push(mapper(index));
    }
    return rows.join("");
  }

  function _normalizeClassList(value) {
    return String(value || "")
      .split(/\s+/)
      .map((token) => token.trim())
      .filter((token) => /^[A-Za-z0-9_-]+$/.test(token))
      .join(" ");
  }

  function _joinClassNames(...parts) {
    return parts
      .map((part) => _normalizeClassList(part))
      .filter(Boolean)
      .join(" ");
  }

  function ensureSkeletonStyles() {
    if (typeof document === "undefined" || !document.head) return;
    if (document.getElementById(SKELETON_STYLE_ID)) return;

    const styleNode = document.createElement("style");
    styleNode.id = SKELETON_STYLE_ID;
    styleNode.textContent = `
.gd-skeleton-surface {
  position: relative;
  overflow: hidden;
  pointer-events: none !important;
  user-select: none;
}
.gd-skeleton-surface::after {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  background: linear-gradient(108deg, transparent 10%, rgba(255, 255, 255, 0.09) 46%, transparent 76%);
  transform: translateX(-110%);
  animation: gdSkeletonSweep 1.45s ease-in-out infinite;
}
.gd-skeleton-card-shell {
  display: grid;
  grid-template-rows: 132px auto;
  min-height: 318px;
  border-radius: 16px;
  border: 1px solid rgba(130, 173, 247, 0.2);
  background: linear-gradient(180deg, rgba(16, 30, 52, 0.74), rgba(9, 18, 33, 0.86));
}
.gd-skeleton-card-shell.gd-skeleton-compact {
  min-height: 228px;
}
.gd-skeleton-thumb {
  display: block;
  width: 100%;
  height: 132px;
  border-bottom: 1px solid rgba(130, 173, 247, 0.16);
  background: linear-gradient(180deg, rgba(121, 168, 255, 0.26), rgba(121, 168, 255, 0.08));
}
.gd-skeleton-body {
  padding: 12px;
  display: grid;
  gap: 9px;
}
.gd-skeleton-row {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}
.gd-skeleton-grid3 {
  display: grid;
  gap: 8px;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}
.gd-skeleton-block {
  display: block;
  border-radius: 8px;
  border: 1px solid rgba(153, 184, 246, 0.14);
  background: linear-gradient(180deg, rgba(153, 184, 246, 0.2), rgba(153, 184, 246, 0.08));
}
.gd-skeleton-line {
  height: 12px;
}
.gd-skeleton-badge {
  height: 20px;
  border-radius: 999px;
}
.gd-skeleton-w-80 { width: 80%; }
.gd-skeleton-w-72 { width: 72%; }
.gd-skeleton-w-64 { width: 64%; }
.gd-skeleton-w-56 { width: 56%; }
.gd-skeleton-w-48 { width: 48%; }
.gd-skeleton-w-40 { width: 40%; }
.gd-skeleton-w-34 { width: 34%; }
.gd-skeleton-w-30 { width: 30%; }
.gd-skeleton-w-24 { width: 24%; }
.gd-skeleton-radar-item {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr) auto;
  gap: 11px;
  align-items: center;
  min-height: 72px;
  padding: 10px 11px;
  border-radius: 14px;
  border: 1px solid rgba(255, 196, 111, 0.25);
  background: linear-gradient(180deg, rgba(46, 32, 19, 0.6), rgba(21, 17, 12, 0.86));
}
.gd-skeleton-radar-thumb {
  width: 88px;
  height: 48px;
  border-radius: 10px;
  border-bottom: none;
}
.gd-skeleton-radar-main,
.gd-skeleton-radar-side {
  display: grid;
  gap: 8px;
}
.gd-skeleton-radar-side {
  justify-items: end;
}
.gd-skeleton-mini-item {
  display: grid;
  grid-template-columns: 1fr auto;
  align-items: center;
  gap: 10px;
  min-height: 62px;
  padding: 11px 12px;
  border-radius: 14px;
  border: 1px solid rgba(130, 173, 247, 0.2);
  background: linear-gradient(180deg, rgba(21, 35, 58, 0.78), rgba(12, 22, 40, 0.9));
}
.gd-skeleton-search-item {
  display: grid;
  grid-template-columns: 84px minmax(0, 1fr) auto;
  gap: 10px;
  align-items: center;
  min-height: 72px;
  padding: 10px 11px;
  border-bottom: 1px solid rgba(121, 168, 255, 0.14);
}
.gd-skeleton-search-thumb {
  width: 84px;
  height: 46px;
  border-radius: 10px;
  border-bottom: none;
}
.gd-skeleton-panel-row {
  display: grid;
  gap: 9px;
  min-height: 78px;
  padding: 12px;
  border-radius: 14px;
  border: 1px solid rgba(130, 173, 247, 0.2);
  background: linear-gradient(180deg, rgba(16, 30, 52, 0.72), rgba(11, 21, 38, 0.86));
}
.gd-skeleton-meta-row {
  display: grid;
  gap: 8px;
  min-height: 72px;
  padding: 12px;
  border-radius: 12px;
  border: 1px solid rgba(130, 173, 247, 0.2);
  background: linear-gradient(180deg, rgba(16, 30, 52, 0.72), rgba(11, 21, 38, 0.86));
}
@media (max-width: 760px) {
  .gd-skeleton-card-shell {
    grid-template-rows: 118px auto;
    min-height: 272px;
    border-radius: 14px;
  }
  .gd-skeleton-thumb {
    height: 118px;
  }
  .gd-skeleton-body {
    padding: 10px;
    gap: 8px;
  }
  .gd-skeleton-grid3 {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .gd-skeleton-radar-item {
    grid-template-columns: 72px minmax(0, 1fr);
    gap: 9px;
    min-height: 66px;
    padding: 9px 10px;
  }
  .gd-skeleton-radar-thumb {
    width: 72px;
    height: 40px;
  }
  .gd-skeleton-radar-side {
    grid-column: 2;
    justify-items: start;
  }
  .gd-skeleton-search-item {
    grid-template-columns: 72px minmax(0, 1fr);
    gap: 9px;
    min-height: 66px;
    padding: 9px 10px;
  }
  .gd-skeleton-search-thumb {
    width: 72px;
    height: 40px;
  }
  .gd-skeleton-search-item > :last-child {
    grid-column: 2;
    justify-self: start;
  }
}
@media (max-width: 520px) {
  .gd-skeleton-card-shell {
    grid-template-rows: 108px auto;
    min-height: 248px;
  }
  .gd-skeleton-thumb {
    height: 108px;
  }
  .gd-skeleton-grid3 {
    grid-template-columns: 1fr;
  }
}
@keyframes gdSkeletonSweep {
  from { transform: translateX(-110%); }
  to { transform: translateX(110%); }
}
@media (prefers-reduced-motion: reduce) {
  .gd-skeleton-surface::after {
    animation: none;
    opacity: 0;
  }
}
`;
    document.head.appendChild(styleNode);
  }

  function _skeletonDealCardMarkup(options = {}) {
    const cardClass = _normalizeClassList(options.cardClass);
    const compactClass = options.compact ? "gd-skeleton-compact" : "";
    const classes = _joinClassNames(cardClass, "gd-skeleton-card-shell", compactClass, "gd-skeleton-surface");
    return `
<article class="${classes}" aria-hidden="true">
  <div class="gd-skeleton-thumb"></div>
  <div class="gd-skeleton-body">
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-72"></span>
    <div class="gd-skeleton-row">
      <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-34"></span>
      <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-24"></span>
    </div>
    <div class="gd-skeleton-row">
      <span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-40"></span>
      <span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-30"></span>
      <span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-24"></span>
    </div>
    <div class="gd-skeleton-grid3">
      <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-64"></span>
      <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-56"></span>
      <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-48"></span>
    </div>
  </div>
</article>
`;
  }

  function _skeletonMiniItemMarkup(options = {}) {
    const itemClass = _normalizeClassList(options.itemClass);
    const classes = _joinClassNames(itemClass, "gd-skeleton-mini-item", "gd-skeleton-surface");
    return `
<div class="${classes}" aria-hidden="true">
  <div style="display:grid;gap:8px;">
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-64"></span>
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-40"></span>
  </div>
  <span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-34"></span>
</div>
`;
  }

  function _skeletonRadarItemMarkup(options = {}) {
    const itemClass = _normalizeClassList(options.itemClass || "deal-radar-card");
    const thumbClass = _normalizeClassList(options.thumbClass || "deal-radar-thumb");
    const classes = _joinClassNames(itemClass, "gd-skeleton-radar-item", "gd-skeleton-surface");
    const thumbClasses = _joinClassNames(thumbClass, "gd-skeleton-thumb", "gd-skeleton-radar-thumb");
    return `
<div class="${classes}" aria-hidden="true">
  <div class="${thumbClasses}"></div>
  <div class="gd-skeleton-radar-main">
    <span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-40"></span>
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-72"></span>
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-56"></span>
  </div>
  <div class="gd-skeleton-radar-side">
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-48"></span>
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-34"></span>
  </div>
</div>
`;
  }

  function _skeletonSearchItemMarkup(options = {}) {
    const itemClass = _normalizeClassList(options.itemClass || "search-result-item");
    const thumbClass = _normalizeClassList(options.thumbClass || "search-result-thumb");
    const classes = _joinClassNames(itemClass, "gd-skeleton-search-item", "gd-skeleton-surface");
    const thumbClasses = _joinClassNames(thumbClass, "gd-skeleton-thumb", "gd-skeleton-search-thumb");
    return `
<div class="${classes}" aria-hidden="true">
  <div class="${thumbClasses}"></div>
  <div style="display:grid;gap:8px;">
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-72"></span>
    <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-56"></span>
  </div>
  <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-30"></span>
</div>
`;
  }

  function _skeletonPanelRowMarkup(options = {}) {
    const itemClass = _normalizeClassList(options.itemClass);
    const classes = _joinClassNames(itemClass, "gd-skeleton-panel-row", "gd-skeleton-surface");
    return `
<div class="${classes}" aria-hidden="true">
  <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-56"></span>
  <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-80"></span>
  <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-40"></span>
</div>
`;
  }

  function _skeletonMetaRowMarkup(options = {}) {
    const itemClass = _normalizeClassList(options.itemClass);
    const classes = _joinClassNames(itemClass, "gd-skeleton-meta-row", "gd-skeleton-surface");
    return `
<div class="${classes}" aria-hidden="true">
  <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-48"></span>
  <span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-64"></span>
</div>
`;
  }

  function getSkeletonMarkup(kind, count = 1, options = {}) {
    ensureSkeletonStyles();
    const normalizedKind = String(kind || "").trim().toLowerCase();
    const safeCount = _toClampedInt(count, 1, 1, 36);

    switch (normalizedKind) {
      case "deal-cards":
      case "result-cards":
      case "watchlist-cards":
        return _repeatMarkup(safeCount, () => _skeletonDealCardMarkup(options));
      case "mini-list":
        return _repeatMarkup(safeCount, () => _skeletonMiniItemMarkup(options));
      case "radar-list":
        return _repeatMarkup(safeCount, () => _skeletonRadarItemMarkup(options));
      case "search-results":
        return _repeatMarkup(safeCount, () => _skeletonSearchItemMarkup(options));
      case "panel-list":
        return _repeatMarkup(safeCount, () => _skeletonPanelRowMarkup(options));
      case "meta-grid":
        return _repeatMarkup(safeCount, () => _skeletonMetaRowMarkup(options));
      default:
        return _skeletonPanelRowMarkup(options);
    }
  }

  function _resolveSkeletonTarget(target) {
    if (target && target.nodeType === 1) return target;
    if (typeof target === "string" && target) {
      return document.querySelector(target);
    }
    return null;
  }

  function renderSkeleton(target, kind, count = 1, options = {}) {
    const node = _resolveSkeletonTarget(target);
    if (!node) return "";
    const html = getSkeletonMarkup(kind, count, options);
    node.innerHTML = html;
    return html;
  }

  const skeletonApi = Object.freeze({
    ensureStyles: ensureSkeletonStyles,
    markup: getSkeletonMarkup,
    render: renderSkeleton,
  });

  window.GameDenSite = Object.freeze({
    config: siteConfig,
    absoluteUrl,
    getViewerId,
    resolveApiUrl,
    fetchJson,
    applyMetadata,
    getDealConfidence,
    markNewSignal,
    resetNewSignalScope,
    skeleton: skeletonApi,
  });
})();
