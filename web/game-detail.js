const API_BASE = "/games";
const params = new URLSearchParams(window.location.search);
const gameId = params.get("game_id");
const WATCHLIST_STORAGE_KEY = "gameden.user_id";
const DEFAULT_USER_ID = "legacy-user";

let priceChart;
let currentGameDetail = null;
const skeletonUi = window.GameDenSite && window.GameDenSite.skeleton;
const targetPriceInput = document.getElementById("targetPriceInput");
const targetDiscountInput = document.getElementById("targetDiscountInput");
const saveTargetAlertBtn = document.getElementById("saveTargetAlertBtn");
const clearTargetAlertBtn = document.getElementById("clearTargetAlertBtn");
const targetAlertStatus = document.getElementById("targetAlertStatus");

function getCurrentUserId() {
  try {
    const stored = String(localStorage.getItem(WATCHLIST_STORAGE_KEY) || "").trim();
    if (stored) return stored;
    localStorage.setItem(WATCHLIST_STORAGE_KEY, DEFAULT_USER_ID);
    return DEFAULT_USER_ID;
  } catch {
    return DEFAULT_USER_ID;
  }
}

const CURRENT_USER_ID = getCurrentUserId();

if (skeletonUi && typeof skeletonUi.ensureStyles === "function") {
  skeletonUi.ensureStyles();
}

function setInlineSkeleton(id, widthClass = "gd-skeleton-w-56") {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = `<span class="gd-skeleton-block gd-skeleton-line ${widthClass}" aria-hidden="true"></span>`;
}

function renderLoadingSkeletons() {
  setInlineSkeleton("gameTitle", "gd-skeleton-w-64");
  setInlineSkeleton("dealSummary", "gd-skeleton-w-80");
  setInlineSkeleton("currentPrice", "gd-skeleton-w-48");
  setInlineSkeleton("originalPrice", "gd-skeleton-w-40");
  setInlineSkeleton("discountPercent", "gd-skeleton-w-34");
  setInlineSkeleton("historicalLow", "gd-skeleton-w-40");
  setInlineSkeleton("playerCount", "gd-skeleton-w-40");
  setInlineSkeleton("developer", "gd-skeleton-w-56");
  setInlineSkeleton("publisher", "gd-skeleton-w-56");
  setInlineSkeleton("releaseDate", "gd-skeleton-w-48");
  setInlineSkeleton("dealScore", "gd-skeleton-w-48");
  setInlineSkeleton("worthBuyingScore", "gd-skeleton-w-48");
  setInlineSkeleton("momentumScore", "gd-skeleton-w-48");
  setInlineSkeleton("predictionConfidence", "gd-skeleton-w-34");

  const banner = document.getElementById("heroBanner");
  if (banner) {
    banner.classList.add("gd-skeleton-surface");
  }

  const highlights = document.getElementById("dealHighlights");
  if (highlights) {
    highlights.innerHTML = `
      <li><span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-72" aria-hidden="true"></span></li>
      <li><span class="gd-skeleton-block gd-skeleton-line gd-skeleton-w-56" aria-hidden="true"></span></li>
    `;
  }

  const tagList = document.getElementById("tagList");
  if (tagList) {
    tagList.innerHTML = [
      '<span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-30" aria-hidden="true"></span>',
      '<span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-24" aria-hidden="true"></span>',
      '<span class="gd-skeleton-block gd-skeleton-badge gd-skeleton-w-34" aria-hidden="true"></span>',
    ].join("");
  }

  const priceHistoryPanel = document.getElementById("priceChart")?.closest(".panel");
  if (priceHistoryPanel) {
    priceHistoryPanel.classList.add("gd-skeleton-surface");
  }

  if (skeletonUi && typeof skeletonUi.render === "function") {
    skeletonUi.render(document.getElementById("dealFactors"), "panel-list", 4, { itemClass: "factor-item" });
    skeletonUi.render(document.getElementById("predictionPanel"), "panel-list", 5, { itemClass: "prediction-stat" });
    skeletonUi.render(document.getElementById("nextSalePredictionPanel"), "panel-list", 5, { itemClass: "prediction-stat" });
    skeletonUi.render(document.getElementById("buyNowPanel"), "panel-list", 3, { itemClass: "prediction-stat" });
    skeletonUi.render(document.getElementById("dealConfidencePanel"), "panel-list", 3, { itemClass: "prediction-stat" });
    skeletonUi.render(document.getElementById("dealHeatPanel"), "panel-list", 4, { itemClass: "prediction-stat" });
    skeletonUi.render(document.getElementById("marketInsights"), "meta-grid", 6, { itemClass: "meta-item" });
  }
}

function renderLoadFailureState(message) {
  const safeMessage = String(message || "Failed to load game data.");
  setText("gameTitle", safeMessage);
  setText("dealSummary", "Please try refreshing this page.");
  const highlights = document.getElementById("dealHighlights");
  if (highlights) highlights.innerHTML = "";

  const banner = document.getElementById("heroBanner");
  if (banner) {
    banner.classList.remove("gd-skeleton-surface");
  }

  const priceHistoryPanel = document.getElementById("priceChart")?.closest(".panel");
  if (priceHistoryPanel) {
    priceHistoryPanel.classList.remove("gd-skeleton-surface");
  }

  const panelError = `<div class="prediction-stat"><strong>Status</strong><div>${escapeHtml(safeMessage)}</div></div>`;
  ["dealFactors", "predictionPanel", "nextSalePredictionPanel", "buyNowPanel", "dealConfidencePanel", "dealHeatPanel"].forEach((id) => {
    const node = document.getElementById(id);
    if (node) node.innerHTML = panelError;
  });

  const marketNode = document.getElementById("marketInsights");
  if (marketNode) {
    marketNode.innerHTML = `<div class="meta-item"><span>Status</span><strong>${escapeHtml(safeMessage)}</strong></div>`;
  }
}

function parseOptionalNumberInput(inputEl, { integer = false } = {}) {
  if (!(inputEl instanceof HTMLInputElement)) return null;
  const raw = String(inputEl.value || "").trim();
  if (!raw) return null;
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  if (integer) return Math.round(value);
  return Number(value.toFixed(2));
}

function getSuggestedTargetPrice(detail) {
  const low = Number(detail?.historical_low_price);
  if (Number.isFinite(low) && low > 0) return Number(low.toFixed(2));
  const current = Number(detail?.current_price);
  if (Number.isFinite(current) && current > 0) return Number((current * 0.9).toFixed(2));
  return null;
}

function getSuggestedTargetDiscount(detail) {
  const discount = Number(detail?.discount_percent);
  if (!Number.isFinite(discount)) return null;
  return Math.max(0, Math.min(100, Math.round(discount + 10)));
}

function setTargetAlertStatus(message, tone = "muted") {
  if (!targetAlertStatus) return;
  const text = String(message || "").trim();
  targetAlertStatus.textContent = text;
  if (!text || tone === "muted") {
    targetAlertStatus.removeAttribute("data-tone");
    return;
  }
  targetAlertStatus.setAttribute("data-tone", tone);
}

function pulseTargetAlertButton(button) {
  if (!(button instanceof HTMLButtonElement)) return;
  const reduceMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (reduceMotion) return;
  button.classList.remove("is-action-success");
  void button.offsetWidth;
  button.classList.add("is-action-success");
  window.setTimeout(() => button.classList.remove("is-action-success"), 720);
}

function setTargetAlertPending(isPending) {
  const pending = !!isPending;
  [saveTargetAlertBtn, clearTargetAlertBtn].forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) return;
    button.classList.toggle("is-pending", pending);
    button.disabled = pending;
    if (pending) {
      button.setAttribute("aria-busy", "true");
    } else {
      button.removeAttribute("aria-busy");
    }
  });
}

function describeTargetAlert(row) {
  if (!row) return "No target alert set yet.";
  const parts = [];
  const price = Number(row?.target_price);
  const discount = Number(row?.target_discount_percent);
  if (Number.isFinite(price) && price >= 0) parts.push(`price <= $${price.toFixed(2)}`);
  if (Number.isFinite(discount) && discount >= 0) parts.push(`discount >= ${Math.round(discount)}%`);
  if (!parts.length) return "Target alert active.";
  return `Active alert: ${parts.join(" or ")}`;
}

function prefillTargetInputs(detail) {
  if (targetPriceInput instanceof HTMLInputElement && !String(targetPriceInput.value || "").trim()) {
    const suggestedPrice = getSuggestedTargetPrice(detail);
    if (suggestedPrice !== null) {
      targetPriceInput.value = suggestedPrice.toFixed(2);
    }
  }
  if (targetDiscountInput instanceof HTMLInputElement && !String(targetDiscountInput.value || "").trim()) {
    const suggestedDiscount = getSuggestedTargetDiscount(detail);
    if (suggestedDiscount !== null) {
      targetDiscountInput.value = String(suggestedDiscount);
    }
  }
}

async function syncTargetAlert() {
  if (!Number.isFinite(Number(gameId)) || Number(gameId) <= 0) {
    if (clearTargetAlertBtn) clearTargetAlertBtn.hidden = true;
    setTargetAlertStatus("Target alerts require a valid game.", "error");
    return;
  }
  const rows = await fetchJson(`/deal-watchlists/${encodeURIComponent(CURRENT_USER_ID)}`).catch(() => []);
  const row = Array.isArray(rows)
    ? rows.find((item) => Number(item?.game_id) === Number(gameId) && item?.active !== false)
    : null;

  if (row) {
    if (targetPriceInput instanceof HTMLInputElement) {
      targetPriceInput.value = row.target_price !== null && row.target_price !== undefined
        ? Number(row.target_price).toFixed(2)
        : "";
    }
    if (targetDiscountInput instanceof HTMLInputElement) {
      targetDiscountInput.value = row.target_discount_percent !== null && row.target_discount_percent !== undefined
        ? String(Math.round(Number(row.target_discount_percent)))
        : "";
    }
    if (clearTargetAlertBtn) clearTargetAlertBtn.hidden = false;
    setTargetAlertStatus(describeTargetAlert(row));
    return;
  }

  if (clearTargetAlertBtn) clearTargetAlertBtn.hidden = true;
  prefillTargetInputs(currentGameDetail);
  setTargetAlertStatus("No target alert set yet.");
}

function readTargetAlertForm() {
  const targetPrice = parseOptionalNumberInput(targetPriceInput);
  const targetDiscountPercent = parseOptionalNumberInput(targetDiscountInput, { integer: true });
  if (targetPrice !== null && targetPrice < 0) {
    throw new Error("Target price must be 0 or greater.");
  }
  if (targetDiscountPercent !== null && (targetDiscountPercent < 0 || targetDiscountPercent > 100)) {
    throw new Error("Target discount must be between 0 and 100.");
  }
  if (targetPrice === null && targetDiscountPercent === null) {
    throw new Error("Enter a target price or target discount.");
  }
  return { targetPrice, targetDiscountPercent };
}

async function saveTargetAlert() {
  if (!Number.isFinite(Number(gameId)) || Number(gameId) <= 0) {
    setTargetAlertStatus("Target alerts require a valid game.", "error");
    return;
  }
  const { targetPrice, targetDiscountPercent } = readTargetAlertForm();
  setTargetAlertPending(true);
  try {
    const row = await fetchJson("/deal-watchlists/add", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: CURRENT_USER_ID,
        game_id: Number(gameId),
        target_price: targetPrice,
        target_discount_percent: targetDiscountPercent,
      }),
    });
    if (clearTargetAlertBtn) clearTargetAlertBtn.hidden = false;
    setTargetAlertStatus(describeTargetAlert(row || null), "success");
    pulseTargetAlertButton(saveTargetAlertBtn);
  } finally {
    setTargetAlertPending(false);
  }
}

async function clearTargetAlert() {
  if (!Number.isFinite(Number(gameId)) || Number(gameId) <= 0) return;
  setTargetAlertPending(true);
  try {
    await fetchJson("/deal-watchlists/remove", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: CURRENT_USER_ID,
        game_id: Number(gameId),
      }),
    });
    if (targetPriceInput instanceof HTMLInputElement) targetPriceInput.value = "";
    if (targetDiscountInput instanceof HTMLInputElement) targetDiscountInput.value = "";
    if (clearTargetAlertBtn) clearTargetAlertBtn.hidden = true;
    prefillTargetInputs(currentGameDetail);
    setTargetAlertStatus("Target alert removed.", "success");
    pulseTargetAlertButton(clearTargetAlertBtn);
  } finally {
    setTargetAlertPending(false);
  }
}

function fmtPrice(value) {
  if (value === null || value === undefined) return "-";
  return `$${Number(value).toFixed(2)}`;
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value ?? "-";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normalizeReasonSnippet(value) {
  const raw = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!raw) return "";

  const lower = raw.toLowerCase();
  if (lower.includes("historical low")) return "Near historical low";
  if (
    (lower.includes("player") || lower.includes("momentum"))
    && (
      lower.includes("up")
      || lower.includes("growth")
      || lower.includes("rising")
      || lower.includes("surge")
      || lower.includes("climbing")
    )
  ) {
    return "Strong player growth";
  }
  if (
    lower.includes("discount")
    || lower.includes("sale")
    || lower.includes("price favorable")
    || lower.includes("better sale")
  ) {
    return "Large discount vs normal price";
  }
  if (
    lower.includes("popular")
    || lower.includes("trending")
    || lower.includes("interest")
  ) {
    return "Popular game currently trending";
  }

  const firstSentence = raw
    .split(/[.!?]/)
    .map((segment) => segment.trim())
    .find(Boolean) || raw;
  const compact = firstSentence.length > 74
    ? `${firstSentence.slice(0, 71).trimEnd()}...`
    : firstSentence;
  return compact.charAt(0).toUpperCase() + compact.slice(1);
}

function resolveDealConfidence(detail) {
  const helper = window.GameDenSite && window.GameDenSite.getDealConfidence;
  if (typeof helper === "function") {
    return helper(detail);
  }

  const candidates = [
    detail?.buy_score,
    detail?.worth_buying?.score,
    detail?.worth_buying_score,
    detail?.deal_score,
  ];
  let score = null;
  for (const candidate of candidates) {
    const parsed = Number(candidate);
    if (!Number.isFinite(parsed)) continue;
    score = Math.max(0, Math.min(100, parsed));
    break;
  }
  if (score === null) return null;

  if (score >= 85) return { score, confidence_label: "Strong Buy", confidence_icon: "SB", class_name: "strong-buy" };
  if (score >= 70) return { score, confidence_label: "Good Deal", confidence_icon: "GD", class_name: "good-deal" };
  if (score >= 50) return { score, confidence_label: "Fair Price", confidence_icon: "FP", class_name: "fair-price" };
  return { score, confidence_label: "Wait", confidence_icon: "WT", class_name: "wait" };
}

function buildDealConfidenceExplanation(detail) {
  const lines = [];
  const push = (value) => {
    const text = normalizeReasonSnippet(value);
    if (!text) return;
    const token = text.toLowerCase();
    if (lines.some((line) => String(line).toLowerCase() === token)) return;
    lines.push(text);
  };

  push(detail?.buy_reason);
  push(detail?.predicted_sale_reason ?? detail?.next_sale_prediction?.reason);
  push(detail?.deal_heat?.reason ?? detail?.deal_heat_reason);
  return lines.slice(0, 2);
}

function pushUniqueHighlight(lines, line) {
  const normalized = String(line || "").trim();
  if (!normalized) return;
  const token = normalized.toLowerCase();
  if (lines.some((entry) => String(entry).toLowerCase() === token)) return;
  lines.push(normalized);
}

function buildDetailHighlightLines(detail) {
  const lines = [];
  const recommendation = String(detail?.buy_recommendation || "").trim().toUpperCase();
  const buyReason = normalizeReasonSnippet(detail?.buy_reason);
  const predictedReason = normalizeReasonSnippet(detail?.predicted_sale_reason ?? detail?.next_sale_prediction?.reason);
  const worthReason = normalizeReasonSnippet(detail?.worth_buying?.reason ?? detail?.worth_buying_reason_summary);
  const trendReason = normalizeReasonSnippet(detail?.momentum?.reason ?? detail?.trend_reason_summary);
  const heatReason = normalizeReasonSnippet(detail?.deal_heat?.reason ?? detail?.deal_heat_reason);
  const lowRadarReason = normalizeReasonSnippet(detail?.historical_low_radar?.reason);
  const discount = Math.max(0, Number(detail?.discount_percent ?? 0) || 0);
  const currentPrice = Number(detail?.current_price);
  const historicalLow = Number(detail?.historical_low_price);
  const priceVsLowRatio = Number(detail?.price_vs_low_ratio);
  const growthRatio = Number(detail?.momentum?.player_growth_ratio);
  const heatLevel = String(detail?.deal_heat?.level || "").trim().toUpperCase();

  if (recommendation === "BUY_NOW") {
    pushUniqueHighlight(lines, buyReason || "Buy-now timing signal");
  } else if (recommendation === "WAIT") {
    pushUniqueHighlight(lines, buyReason || predictedReason || "Better value likely on next sale");
  }

  if (detail?.historical_low_radar?.hit) {
    pushUniqueHighlight(lines, "Near historical low");
  } else if (
    Number.isFinite(currentPrice)
    && Number.isFinite(historicalLow)
    && historicalLow > 0
    && currentPrice <= historicalLow * 1.08
  ) {
    pushUniqueHighlight(lines, "Near historical low");
  } else if (Number.isFinite(priceVsLowRatio) && priceVsLowRatio > 0 && priceVsLowRatio <= 1.08) {
    pushUniqueHighlight(lines, "Near historical low");
  } else if (lowRadarReason) {
    pushUniqueHighlight(lines, lowRadarReason);
  }

  if (trendReason) {
    pushUniqueHighlight(lines, trendReason);
  } else if (Number.isFinite(growthRatio) && growthRatio >= 1.1) {
    pushUniqueHighlight(lines, "Strong player growth");
  }

  if (discount >= 60) {
    pushUniqueHighlight(lines, "Large discount vs normal price");
  } else if (discount >= 35) {
    pushUniqueHighlight(lines, "Meaningful discount right now");
  }

  if (heatReason) {
    pushUniqueHighlight(lines, heatReason);
  } else if (heatLevel === "HOT") {
    pushUniqueHighlight(lines, "Popular game currently trending");
  }

  if (!lines.length && worthReason) {
    pushUniqueHighlight(lines, worthReason);
  }
  if (!lines.length && predictedReason) {
    pushUniqueHighlight(lines, predictedReason);
  }

  return lines.slice(0, 3);
}

function renderDealHighlights(detail) {
  const list = document.getElementById("dealHighlights");
  if (!list) return;
  const lines = buildDetailHighlightLines(detail);
  if (!lines.length) {
    list.innerHTML = "";
    return;
  }
  list.innerHTML = lines.map((line) => `<li>${escapeHtml(line)}</li>`).join("");
}

async function fetchJson(url, options = {}) {
  if (!window.GameDenSite || typeof window.GameDenSite.fetchJson !== "function") {
    throw new Error("GameDen runtime is not initialized. Ensure /site-branding.js loads before page scripts.");
  }
  return window.GameDenSite.fetchJson(url, options);
}

function renderDetail(detail) {
  currentGameDetail = detail || null;
  setText("gameTitle", detail.name);
  setText("dealSummary", detail.deal_summary);
  setText("currentPrice", fmtPrice(detail.current_price));
  setText("originalPrice", fmtPrice(detail.original_price));
  setText("discountPercent", detail.discount_percent != null ? `${detail.discount_percent}%` : "-");
  setText("historicalLow", fmtPrice(detail.historical_low_price));
  setText("playerCount", detail.current_players != null ? Number(detail.current_players).toLocaleString() : "-");
  setText("developer", detail.developer || "-");
  setText("publisher", detail.publisher || "-");
  setText("releaseDate", detail.release_date ? new Date(detail.release_date).toLocaleDateString() : "-");
  setText("dealScore", `${detail.deal_score} (${detail.deal_label})`);
  setText("worthBuyingScore", detail.worth_buying?.score != null ? Number(detail.worth_buying.score).toFixed(1) : "-");
  setText("momentumScore", detail.momentum?.score != null ? Number(detail.momentum.score).toFixed(1) : "-");
  const confidence = resolveDealConfidence(detail);
  setText("predictionConfidence", confidence ? confidence.confidence_label : (detail.prediction?.confidence || "-"));
  renderDealHighlights(detail);

  const banner = document.getElementById("heroBanner");
  if (banner) {
    banner.classList.remove("gd-skeleton-surface");
  }
  if (banner && detail.banner_image) {
    banner.style.backgroundImage = `url('${detail.banner_image}')`;
  }

  const tagList = document.getElementById("tagList");
  tagList.innerHTML = (detail.tags || []).map((tag) => `<span class="tag">${tag}</span>`).join("");

  const market = detail.market_insights || {};
  const entries = [
    ["Historical Low", fmtPrice(market.historical_low_price)],
    ["Avg Discount", market.avg_discount_percent != null ? `${market.avg_discount_percent}%` : "-"],
    ["Max Discount", market.max_discount_percent != null ? `${market.max_discount_percent}%` : "-"],
    ["Sale Events", market.sale_event_count != null ? market.sale_event_count : "-"],
    ["Days Since Last Sale", market.days_since_last_sale != null ? market.days_since_last_sale : "-"],
    ["Latest Players", market.latest_player_count != null ? Number(market.latest_player_count).toLocaleString() : "-"],
  ];

  document.getElementById("marketInsights").innerHTML = entries
    .map(([label, value]) => `<div class="meta-item"><span>${label}</span><strong>${value}</strong></div>`)
    .join("");

  renderPrediction(detail.prediction || {});
  renderNextSalePrediction(detail);
  renderBuyRecommendation(detail);
  renderDealConfidence(detail);
  renderDealHeat(detail.deal_heat || {}, detail.worth_buying || {}, detail.momentum || {}, detail.historical_low_radar || {});
}

function renderPrediction(prediction) {
  const reasons = (prediction.reasoning || [])
    .map((line) => `<li>${line}</li>`)
    .join("");

  document.getElementById("predictionPanel").innerHTML = `
    <div class="prediction-stat"><strong>7D Probability</strong><div>${Math.round((prediction.sale_probability_7d || 0) * 100)}%</div></div>
    <div class="prediction-stat"><strong>30D Probability</strong><div>${Math.round((prediction.sale_probability_30d || 0) * 100)}%</div></div>
    <div class="prediction-stat"><strong>Predicted Discount</strong><div>${prediction.predicted_discount_percent != null ? prediction.predicted_discount_percent + "%" : "-"}</div></div>
    <div class="prediction-stat"><strong>Predicted Window</strong><div>${prediction.predicted_sale_window_start ? new Date(prediction.predicted_sale_window_start).toLocaleDateString() : "-"} - ${prediction.predicted_sale_window_end ? new Date(prediction.predicted_sale_window_end).toLocaleDateString() : "-"}</div></div>
    <div class="prediction-stat"><strong>Reasoning</strong><ul class="reasoning-list">${reasons}</ul></div>
  `;
}

function renderNextSalePrediction(detail) {
  const panel = document.getElementById("nextSalePredictionPanel");
  if (!panel) return;

  const prediction = detail.next_sale_prediction || {};
  const expectedNextPrice = prediction.expected_next_price ?? detail.predicted_next_sale_price;
  const expectedNextDiscount = prediction.expected_next_discount_percent ?? detail.predicted_next_discount_percent;
  const windowMin = prediction.estimated_window_days_min ?? detail.predicted_next_sale_window_days_min;
  const windowMax = prediction.estimated_window_days_max ?? detail.predicted_next_sale_window_days_max;
  const confidenceRaw = String(prediction.confidence ?? detail.predicted_sale_confidence ?? "").trim();
  const reason = String(prediction.reason ?? detail.predicted_sale_reason ?? "").trim();

  const hasWindow = Number.isFinite(Number(windowMin)) && Number.isFinite(Number(windowMax));
  const windowLabel = hasWindow
    ? `${Math.max(0, Number(windowMin))}-${Math.max(0, Number(windowMax))} days`
    : "-";
  const confidenceLabel = confidenceRaw
    ? `${confidenceRaw.slice(0, 1).toUpperCase()}${confidenceRaw.slice(1).toLowerCase()}`
    : "-";

  if (
    expectedNextPrice == null
    && expectedNextDiscount == null
    && !hasWindow
    && !reason
  ) {
    panel.innerHTML = `
      <div class="prediction-stat"><strong>Expected Next Price</strong><div>-</div></div>
      <div class="prediction-stat"><strong>Expected Discount</strong><div>-</div></div>
      <div class="prediction-stat"><strong>Estimated Timing</strong><div>-</div></div>
      <div class="prediction-stat"><strong>Confidence</strong><div>Low</div></div>
      <div class="prediction-stat"><strong>Reason</strong><div>Not enough discount history for a strong prediction.</div></div>
    `;
    return;
  }

  panel.innerHTML = `
    <div class="prediction-stat"><strong>Expected Next Price</strong><div>${escapeHtml(fmtPrice(expectedNextPrice))}</div></div>
    <div class="prediction-stat"><strong>Expected Discount</strong><div>${expectedNextDiscount != null ? escapeHtml(`${expectedNextDiscount}%`) : "-"}</div></div>
    <div class="prediction-stat"><strong>Estimated Timing</strong><div>${escapeHtml(windowLabel)}</div></div>
    <div class="prediction-stat"><strong>Confidence</strong><div>${escapeHtml(confidenceLabel)}</div></div>
    <div class="prediction-stat"><strong>Reason</strong><div>${escapeHtml(reason || "Not enough discount history for a strong prediction.")}</div></div>
  `;
}

function renderDealHeat(heat, worth, momentum, lowRadar) {
  const tags = Array.isArray(heat.tags) ? heat.tags : [];
  document.getElementById("dealHeatPanel").innerHTML = `
    <div class="prediction-stat"><strong>Heat Level</strong><div>${heat.level || "-"}</div></div>
    <div class="prediction-stat"><strong>Why This Is Hot</strong><div>${heat.reason || "-"}</div></div>
    <div class="prediction-stat"><strong>Worth Buying Reason</strong><div>${worth.reason || "-"}</div></div>
    <div class="prediction-stat"><strong>Trend Reason</strong><div>${momentum.reason || "-"}</div></div>
    <div class="prediction-stat"><strong>Historical Low Radar</strong><div>${lowRadar.reason || "-"}</div></div>
    <div class="prediction-stat"><strong>Heat Tags</strong><div>${tags.length ? tags.join(", ") : "-"}</div></div>
  `;
}

function renderBuyRecommendation(detail) {
  const panel = document.getElementById("buyNowPanel");
  if (!panel) return;

  const recommendation = String(detail.buy_recommendation || "").trim().toUpperCase();
  const reason = String(detail.buy_reason || "").trim();
  const ratioValue = Number(detail.price_vs_low_ratio);
  const hasRatio = Number.isFinite(ratioValue) && ratioValue > 0;

  if (!recommendation) {
    panel.innerHTML = `
      <div class="prediction-stat"><strong>Recommendation</strong><div>-</div></div>
      <div class="prediction-stat"><strong>Reason</strong><div>Recommendation unavailable for this game yet.</div></div>
    `;
    return;
  }

  const recommendationLabel = recommendation === "BUY_NOW" ? "BUY NOW" : "WAIT";
  let ratioSummary = "-";
  if (hasRatio) {
    if (ratioValue > 1.0) {
      ratioSummary = `${Math.round((ratioValue - 1.0) * 100)}% above historical low`;
    } else if (ratioValue < 1.0) {
      ratioSummary = `${Math.round((1.0 - ratioValue) * 100)}% below historical low`;
    } else {
      ratioSummary = "At historical low";
    }
  }

  panel.innerHTML = `
    <div class="prediction-stat"><strong>Recommendation</strong><div>${escapeHtml(recommendationLabel)}</div></div>
    <div class="prediction-stat"><strong>Reason</strong><div>${escapeHtml(reason || "Snapshot-derived recommendation.")}</div></div>
    <div class="prediction-stat"><strong>Price vs Historical Low</strong><div>${escapeHtml(ratioSummary)}</div></div>
  `;
}

function renderDealConfidence(detail) {
  const panel = document.getElementById("dealConfidencePanel");
  if (!panel) return;

  const confidence = resolveDealConfidence(detail);
  if (!confidence) {
    panel.innerHTML = `
      <div class="prediction-stat"><strong>Tier</strong><div>-</div></div>
      <div class="prediction-stat"><strong>Why</strong><div>Confidence is unavailable until score data is populated.</div></div>
    `;
    return;
  }

  const explanationLines = buildDealConfidenceExplanation(detail);
  const explanationText = explanationLines.length
    ? explanationLines.join(" • ")
    : "Signals are mixed and a better entry may appear later.";
  const score = Number(confidence.score);
  const scoreText = Number.isFinite(score) ? `${Math.round(score)}/100` : "-";
  const tone = String(confidence.class_name || "wait").trim() || "wait";
  const icon = String(confidence.confidence_icon || "").trim();
  const label = String(confidence.confidence_label || "Deal Confidence").trim();

  panel.innerHTML = `
    <div class="prediction-stat"><strong>Tier</strong><div><span class="confidence-pill ${escapeHtml(tone)}">${escapeHtml(icon)} ${escapeHtml(label)}</span></div></div>
    <div class="prediction-stat"><strong>Score</strong><div>${escapeHtml(scoreText)}</div></div>
    <div class="prediction-stat"><strong>Why</strong><div>${escapeHtml(explanationText)}</div></div>
  `;
}

function renderDealFactors(data) {
  const factors = data.factors || [];
  document.getElementById("dealFactors").innerHTML = factors
    .map((factor) => {
      const pct = factor.max_score ? Math.max(0, Math.min((factor.score / factor.max_score) * 100, 100)) : 0;
      return `
        <div class="factor-item">
          <div class="factor-top"><strong>${factor.name}</strong><span>${factor.score} / ${factor.max_score}</span></div>
          <div class="factor-bar"><div class="factor-bar-fill" style="width:${pct}%"></div></div>
          <div>${factor.explanation}</div>
        </div>
      `;
    })
    .join("");
}

function renderPriceChart(data) {
  const labels = (data.points || []).map((p) => new Date(p.timestamp).toLocaleDateString());
  const prices = (data.points || []).map((p) => p.price);
  const priceHistoryPanel = document.getElementById("priceChart")?.closest(".panel");
  if (priceHistoryPanel) {
    priceHistoryPanel.classList.remove("gd-skeleton-surface");
  }

  const ctx = document.getElementById("priceChart").getContext("2d");
  if (priceChart) priceChart.destroy();

  priceChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Price",
        data: prices,
        borderColor: "#7dd3fc",
        backgroundColor: "rgba(125, 211, 252, 0.2)",
        tension: 0.25,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        y: {
          ticks: {
            callback: (value) => `$${value}`,
          },
        },
      },
    },
  });
}

async function loadPriceHistory(rangeKey = "90d") {
  const data = await fetchJson(`${API_BASE}/${gameId}/price-history?range=${rangeKey}`);
  renderPriceChart(data);
}

async function loadDealExplanation() {
  const data = await fetchJson(`${API_BASE}/${gameId}/deal-explanation`);
  renderDealFactors(data);
}

function bindRangeButtons() {
  const buttons = document.querySelectorAll(".range-btn");
  buttons.forEach((button) => {
    button.addEventListener("click", async () => {
      buttons.forEach((btn) => btn.classList.remove("active"));
      button.classList.add("active");
      await loadPriceHistory(button.dataset.range);
    });
  });
}

function bindTargetAlertControls() {
  saveTargetAlertBtn?.addEventListener("click", () => {
    void saveTargetAlert().catch((error) => {
      console.error(error);
      setTargetAlertStatus(error instanceof Error ? error.message : "Failed to save target alert.", "error");
    });
  });

  clearTargetAlertBtn?.addEventListener("click", () => {
    void clearTargetAlert().catch((error) => {
      console.error(error);
      setTargetAlertStatus("Failed to remove target alert.", "error");
    });
  });

  [targetPriceInput, targetDiscountInput].forEach((input) => {
    input?.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      void saveTargetAlert().catch((error) => {
        console.error(error);
        setTargetAlertStatus(error instanceof Error ? error.message : "Failed to save target alert.", "error");
      });
    });
  });
}

async function init() {
  renderLoadingSkeletons();
  setTargetAlertStatus("Set a target price or discount.");
  bindTargetAlertControls();

  if (!gameId) {
    renderLoadFailureState("Missing game_id query parameter.");
    setTargetAlertStatus("Target alerts require a valid game.", "error");
    return;
  }

  try {
    const detail = await fetchJson(`${API_BASE}/${gameId}`);
    renderDetail(detail);

    await Promise.all([
      loadPriceHistory("90d"),
      loadDealExplanation(),
      syncTargetAlert(),
    ]);

    bindRangeButtons();
  } catch (err) {
    console.error(err);
    renderLoadFailureState("Failed to load game data.");
    setTargetAlertStatus("Failed to load target alert settings.", "error");
  }
}

init();
