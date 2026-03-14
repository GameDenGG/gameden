(function () {
  "use strict";

  const fallbackConfig = Object.freeze({
    site_name: "GameDen.gg",
    site_url: "https://gameden.gg",
    site_description: "Discover game deals, analytics, player trends, and price history on GameDen.gg.",
    // Optional API base for static deployments, e.g. "https://api.gameden.gg"
    api_base: "",
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
  const siteConfig = Object.freeze({
    site_name: String(runtimeConfig.site_name || fallbackConfig.site_name).trim() || fallbackConfig.site_name,
    site_url: normalizeSiteUrl(runtimeConfig.site_url || fallbackConfig.site_url),
    site_description: String(runtimeConfig.site_description || fallbackConfig.site_description).trim() || fallbackConfig.site_description,
    api_base: String(runtimeConfig.api_base || fallbackConfig.api_base).trim(),
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

    const apiBase = String(siteConfig.api_base || "").trim().replace(/\/+$/, "");
    if (!apiBase) {
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

  async function fetchJson(url, options = {}) {
    const requestUrl = resolveApiUrl(url);
    const response = await fetch(requestUrl, options);
    if (response.status === 204) return null;

    const rawBody = await response.text();
    const parsed = _parseJsonBody(rawBody, url, response.status, response.ok);

    if (!response.ok) {
      const fallbackMessage = `Failed to load ${url}: ${response.status}`;
      const error = new Error(_extractErrorDetail(parsed.payload, parsed.rawBody, fallbackMessage));
      error.status = response.status;
      error.url = url;
      throw error;
    }

    return parsed.hasBody ? parsed.payload : null;
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
    updateMetaTag("link[rel='canonical']", "href", canonicalUrl);
  }

  window.GameDenSite = Object.freeze({
    config: siteConfig,
    absoluteUrl,
    resolveApiUrl,
    fetchJson,
    applyMetadata,
  });
})();
