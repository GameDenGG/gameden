const API_BASE = "/games";
const params = new URLSearchParams(window.location.search);
const gameId = params.get("game_id");

let priceChart;

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

async function fetchJson(url) {
  return window.GameDenSite.fetchJson(url);
}

function renderDetail(detail) {
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
  setText("predictionConfidence", detail.prediction?.confidence || "-");

  const banner = document.getElementById("heroBanner");
  if (detail.banner_image) {
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

async function init() {
  if (!gameId) {
    setText("gameTitle", "Missing game_id query parameter");
    return;
  }

  try {
    const detail = await fetchJson(`${API_BASE}/${gameId}`);
    renderDetail(detail);

    await Promise.all([
      loadPriceHistory("90d"),
      loadDealExplanation(),
    ]);

    bindRangeButtons();
  } catch (err) {
    console.error(err);
    setText("gameTitle", "Failed to load game data");
  }
}

init();
