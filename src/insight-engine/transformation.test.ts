import assert from "node:assert/strict";
import test from "node:test";

import { DEBUG_PRIORITY, DISCARD_REASON, INSIGHT_ACTION, TRIGGER_TYPE } from "./constants";
import {
  transformCandidate,
  transformPriceChangeCandidate,
  transformReleaseEventCandidate,
  transformRelevanceIncreaseCandidate,
  transformReviewChangeCandidate,
  transformStatusChangeCandidate,
  validateInsightOutputConstraints,
} from "./transformation";
import type { ScoredCandidate } from "./types";

function baseCandidate(trigger: ScoredCandidate["trigger"]): ScoredCandidate {
  return {
    trigger,
    magnitude: 0.5,
    recency: 0.5,
    user_relevance: 0.35,
    historical_context: 0.3,
    score: 0.61,
    priority: DEBUG_PRIORITY.MEDIUM,
  };
}

test("transformPriceChangeCandidate builds a direct buy insight", () => {
  const insight = transformPriceChangeCandidate(
    baseCandidate({
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId: "game-price",
      timestamp: 1,
      previous: 59.99,
      current: 29.99,
    }),
  );

  assert.equal(insight.type, TRIGGER_TYPE.PRICE_CHANGE);
  assert.equal(insight.gameId, "game-price");
  assert.equal(insight.title, "Buy now at $29.99 after $59.99 drop");
  assert.equal(insight.reason, "Price fell from $59.99 to $29.99");
  assert.equal(insight.action, INSIGHT_ACTION.BUY_NOW);
  assert.equal(insight.score, 0.61);
  assert.equal(validateInsightOutputConstraints(insight), true);
});

test("transformReviewChangeCandidate builds a review improvement insight", () => {
  const insight = transformReviewChangeCandidate(
    baseCandidate({
      type: TRIGGER_TYPE.REVIEW_CHANGE,
      gameId: "game-review",
      timestamp: 1,
      previous: "Mixed",
      current: "Mostly Positive",
    }),
  );

  assert.equal(insight.type, TRIGGER_TYPE.REVIEW_CHANGE);
  assert.equal(insight.title, "Reviews: Mixed → Mostly Positive");
  assert.equal(insight.title.includes("improved"), false);
  assert.equal(insight.reason, "Shifted from Mixed to Mostly Positive");
  assert.equal(insight.action, INSIGHT_ACTION.WAIT);
  assert.equal(validateInsightOutputConstraints(insight), true);
});

test("transformReleaseEventCandidate builds a released insight", () => {
  const insight = transformReleaseEventCandidate(
    baseCandidate({
      type: TRIGGER_TYPE.RELEASE_EVENT,
      gameId: "game-release",
      timestamp: 1,
      previous: null,
      current: "released",
    }),
  );

  assert.equal(insight.type, TRIGGER_TYPE.RELEASE_EVENT);
  assert.equal(insight.title, "Now fully released");
  assert.equal(insight.reason, "Moved from pre-release to released");
  assert.equal(insight.action, INSIGHT_ACTION.WAIT);
  assert.equal(validateInsightOutputConstraints(insight), true);
});

test("transformRelevanceIncreaseCandidate builds a threshold-crossing insight", () => {
  const insight = transformRelevanceIncreaseCandidate(
    baseCandidate({
      type: TRIGGER_TYPE.RELEVANCE_INCREASE,
      gameId: "game-relevance",
      timestamp: 1,
      previous: 0.42,
      current: 0.61,
    }),
  );

  assert.equal(insight.type, TRIGGER_TYPE.RELEVANCE_INCREASE);
  assert.equal(insight.title, "New strong match for you");
  assert.equal(insight.reason, "Relevance crossed the attention threshold");
  assert.equal(insight.action, INSIGHT_ACTION.WAIT);
  assert.equal(validateInsightOutputConstraints(insight), true);
});

test("transformStatusChangeCandidate builds a delisting insight", () => {
  const insight = transformStatusChangeCandidate(
    baseCandidate({
      type: TRIGGER_TYPE.STATUS_CHANGE,
      gameId: "game-status",
      timestamp: 1,
      previous: "stable",
      current: "delisting",
    }),
  );

  assert.equal(insight.type, TRIGGER_TYPE.STATUS_CHANGE);
  assert.equal(insight.title, "Delisting soon");
  assert.equal(insight.reason, "This game is heading for delisting");
  assert.equal(insight.action, INSIGHT_ACTION.BUY_NOW);
  assert.equal(validateInsightOutputConstraints(insight), true);
});

test("transformCandidate preserves scored fields and debug data on success", () => {
  const candidate = baseCandidate({
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "game-success",
    timestamp: 1,
    previous: 100,
    current: 75,
  });

  const result = transformCandidate(candidate);

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "game-success");
  assert.equal(result.debug.type, TRIGGER_TYPE.PRICE_CHANGE);
  assert.equal(result.debug.magnitude, candidate.magnitude);
  assert.equal(result.debug.recency, candidate.recency);
  assert.equal(result.debug.user_relevance, candidate.user_relevance);
  assert.equal(result.debug.historical_context, candidate.historical_context);
  assert.equal(result.debug.score, candidate.score);
  assert.equal(result.debug.priority, candidate.priority);
  assert.equal(result.debug.discardReason, null);
  assert.deepEqual(result.candidate.trigger, candidate.trigger);
  assert.deepEqual(result.candidate.insight, {
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "game-success",
    title: "Buy now at $75 after $100 drop",
    reason: "Price fell from $100 to $75",
    action: INSIGHT_ACTION.BUY_NOW,
    score: candidate.score,
  });
});

test("validateInsightOutputConstraints rejects overlong title, reason, and action plus empty reason", () => {
  assert.equal(
    validateInsightOutputConstraints({
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId: "game-long-title",
      title: "x".repeat(51),
      reason: "valid reason",
      action: INSIGHT_ACTION.BUY_NOW,
      score: 0.5,
    }),
    false,
  );
  assert.equal(
    validateInsightOutputConstraints({
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId: "game-long-reason",
      title: "Valid title",
      reason: "x".repeat(121),
      action: INSIGHT_ACTION.BUY_NOW,
      score: 0.5,
    }),
    false,
  );
  assert.equal(
    validateInsightOutputConstraints({
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId: "game-empty-reason",
      title: "Valid title",
      reason: "",
      action: INSIGHT_ACTION.BUY_NOW,
      score: 0.5,
    }),
    false,
  );
  assert.equal(
    validateInsightOutputConstraints({
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId: "game-long-action",
      title: "Valid title",
      reason: "Valid reason",
      action: "x".repeat(31),
      score: 0.5,
    }),
    false,
  );
});

test("transformCandidate discards overlong titles with overflow", () => {
  const result = transformCandidate({
    trigger: {
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId: "game-overflow",
      timestamp: 1,
      previous: Number.MAX_SAFE_INTEGER,
      current: Number.MAX_SAFE_INTEGER,
    },
    magnitude: 0.5,
    recency: 0.5,
    user_relevance: 0.35,
    historical_context: 0.3,
    score: 0.61,
    priority: DEBUG_PRIORITY.HIGH,
  });

  assert.equal(result.ok, false);
  if (!result.ok) {
    assert.equal(result.debug.gameId, "game-overflow");
    assert.equal(result.debug.type, TRIGGER_TYPE.PRICE_CHANGE);
    assert.equal(result.debug.magnitude, 0.5);
    assert.equal(result.debug.recency, 0.5);
    assert.equal(result.debug.user_relevance, 0.35);
    assert.equal(result.debug.historical_context, 0.3);
    assert.equal(result.debug.score, 0.61);
    assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
    assert.equal(result.debug.discardReason, DISCARD_REASON.OVERFLOW);
  }
});
