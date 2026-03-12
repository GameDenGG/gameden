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

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to load ${url}: ${res.status}`);
  return res.json();
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
