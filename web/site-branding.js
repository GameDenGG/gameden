(function () {
  "use strict";

  const fallbackConfig = Object.freeze({
    site_name: "GameDen.gg",
    site_url: "https://gameden.gg",
    site_description: "Discover game deals, analytics, player trends, and price history on GameDen.gg.",
    // Optional API origin for static deployments, e.g. "https://api.gameden.gg"
    api_base: "",
  });

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
    api_base: String(runtimeConfig.api_base || runtimeConfig.api_origin || fallbackConfig.api_base).trim(),
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
    if (!value || /^(https?:)?\/\//i.test(value)) {
      return value;
    }

    if (/^(mailto:|tel:|data:|javascript:)/i.test(value)) {
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
      if (path.startsWith("/")) {
        return path;
      }
      if (path.startsWith("./")) {
        return `/${path.slice(2)}`;
      }
      return `/${path}`;
    })(rawPath).replace(/^\/+/, "/");

    const normalizedLower = normalizedPath.toLowerCase();
    const staticPagePaths = new Set([
      "/",
      "/index.html",
      "/game.html",
      "/history.html",
      "/watchlist.html",
      "/all-results.html",
      "/game-detail.html",
    ]);
    const staticAssetExtensions = [
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
    const isStaticPage = staticPagePaths.has(normalizedLower);
    const isStaticAsset = staticAssetExtensions.some((ext) => normalizedLower.endsWith(ext));
    if (isStaticPage || isStaticAsset) {
      return value;
    }

    const apiExactPaths = new Set([
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
    const apiPathPrefixes = [
      "/api/",
      "/dashboard/",
      "/sales/",
      "/games/",
      "/deals/",
      "/leaderboards/",
      "/deal-watchlists/",
      "/notifications/",
    ];
    const isApiRoute =
      apiExactPaths.has(normalizedLower) ||
      apiPathPrefixes.some((prefix) => normalizedLower.startsWith(prefix));

    if (!isApiRoute) {
      return value;
    }

    return `${apiBase}${normalizedPath}${suffix}`;
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
    applyMetadata,
  });
})();
