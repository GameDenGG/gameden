(function () {
  "use strict";

  const fallbackConfig = Object.freeze({
    site_name: "GameDen.gg",
    site_url: "https://gameden.gg",
    site_description: "Discover game deals, analytics, player trends, and price history on GameDen.gg.",
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
    updateMetaTag("link[rel='manifest']", "href", "/site.webmanifest");
  }

  window.GameDenSite = Object.freeze({
    config: siteConfig,
    absoluteUrl,
    applyMetadata,
  });
})();
