import assert from "node:assert/strict";
import test from "node:test";

import { DEBUG_PRIORITY, DISCARD_REASON, INSIGHT_ACTION, TRIGGER_TYPE } from "./constants";
import { runInsightEngine } from "./orchestrator";
import type { EvaluationContext } from "./evaluation";

const baseContext: EvaluationContext = {
  evaluationTimestamp: 1_000_000,
  userSignals: {
    isDismissed: false,
    isWishlisted: false,
    isViewedOrTracked: false,
    tasteMatch: "none",
  },
  historicalContext: {
    priceContext: "all_time_low",
  },
};

function priceTrigger(gameId: string, previous: number, current: number, timestamp = baseContext.evaluationTimestamp) {
  return {
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId,
    timestamp,
    previous,
    current,
  };
}

function reviewTrigger(gameId: string, previous: "Mixed" | "Mostly Positive", current: "Mixed" | "Mostly Positive", timestamp = baseContext.evaluationTimestamp) {
  return {
    type: TRIGGER_TYPE.REVIEW_CHANGE,
    gameId,
    timestamp,
    previous,
    current,
  };
}

function releaseTrigger(gameId: string, current: "released" | "early_access" | "major_patch", timestamp = baseContext.evaluationTimestamp) {
  return {
    type: TRIGGER_TYPE.RELEASE_EVENT,
    gameId,
    timestamp,
    previous: null,
    current,
  };
}

function relevanceTrigger(gameId: string, previous: number, current: number, timestamp = baseContext.evaluationTimestamp) {
  return {
    type: TRIGGER_TYPE.RELEVANCE_INCREASE,
    gameId,
    timestamp,
    previous,
    current,
  };
}

function statusTrigger(gameId: string, current: "delisting" | "leaving_subscription" | "price_lock_ending", timestamp = baseContext.evaluationTimestamp) {
  return {
    type: TRIGGER_TYPE.STATUS_CHANGE,
    gameId,
    timestamp,
    previous: "stable" as const,
    current,
  };
}

test("runInsightEngine returns a valid final output for a simple success path", () => {
  const result = runInsightEngine(
    [
      priceTrigger("game-primary", 100, 50),
      reviewTrigger("game-support-1", "Mixed", "Mostly Positive"),
      releaseTrigger("game-support-2", "released"),
    ],
    baseContext,
  );

  assert.equal(result.output.primaryInsight?.gameId, "game-primary");
  assert.equal(result.output.primaryInsight?.action, INSIGHT_ACTION.BUY_NOW);
  assert.equal(result.output.supportingSignals.length, 2);
  assert.equal(result.output.supportingSignals.some((signal) => signal.gameId === "game-support-1"), true);
  assert.equal(result.output.supportingSignals.some((signal) => signal.gameId === "game-support-2"), true);
  assert.equal(result.debug.length > 0, true);
});

test("runInsightEngine returns the null-output contract for empty input", () => {
  const result = runInsightEngine([], baseContext);

  assert.equal(result.output.primaryInsight, null);
  assert.deepEqual(result.output.supportingSignals, []);
  assert.deepEqual(result.debug, []);
});

test("runInsightEngine records invalid triggers and does not evaluate them", () => {
  const result = runInsightEngine([null, { gameId: "missing-type" }, priceTrigger("game-primary", 100, 50)], baseContext);

  const invalidTriggers = result.debug.filter((record) => record.discardReason === DISCARD_REASON.INVALID_TRIGGER);
  assert.equal(invalidTriggers.length, 2);
  assert.equal(result.output.primaryInsight?.gameId, "game-primary");
});

test("runInsightEngine discards low-score evaluation results before transformation", () => {
  const result = runInsightEngine([priceTrigger("game-low", 100, 99)], baseContext);

  assert.equal(result.output.primaryInsight, null);
  assert.deepEqual(result.output.supportingSignals, []);
  assert.equal(result.debug.some((record) => record.discardReason === DISCARD_REASON.SCORE_BELOW_THRESHOLD), true);
});

test("runInsightEngine propagates transformation overflow, filtering, and final selection overflow", () => {
  const result = runInsightEngine(
    [
      priceTrigger("game-primary", 100, 50),
      priceTrigger("game-high-overflow", 100, 50, baseContext.evaluationTimestamp - 10),
      priceTrigger("game-duplicate", 100, 50, baseContext.evaluationTimestamp - 4 * 60 * 60 * 1000),
      priceTrigger("game-duplicate", 100, 50, baseContext.evaluationTimestamp - 2 * 60 * 60 * 1000),
      reviewTrigger("game-conflict", "Mixed", "Mostly Positive"),
      priceTrigger("game-conflict", 100, 50, baseContext.evaluationTimestamp - 3 * 60 * 60 * 1000),
      priceTrigger("game-transform-overflow", 999999999999999, 499999999999999),
      releaseTrigger("game-support-1", "released"),
      reviewTrigger("game-support-2", "Mixed", "Mostly Positive"),
      relevanceTrigger("game-support-3", 0.42, 0.61),
      statusTrigger("game-support-4", "delisting"),
      releaseTrigger("game-support-5", "major_patch"),
      reviewTrigger("game-support-6", "Mixed", "Mostly Positive"),
    ],
    baseContext,
  );

  assert.equal(result.output.primaryInsight?.gameId, "game-primary");
  assert.equal(result.output.supportingSignals.length, 5);
  assert.equal(result.debug.some((record) => record.discardReason === DISCARD_REASON.OVERFLOW), true);
  assert.equal(result.debug.some((record) => record.discardReason === DISCARD_REASON.DUPLICATE), true);
  assert.equal(result.debug.some((record) => record.discardReason === DISCARD_REASON.CONFLICT), true);
});

test("runInsightEngine keeps output deterministic with one high and capped supporting signals", () => {
  const result = runInsightEngine(
    [
      priceTrigger("game-primary", 100, 50),
      priceTrigger("game-high-overflow", 100, 50, baseContext.evaluationTimestamp - 10),
      releaseTrigger("game-support-1", "released", baseContext.evaluationTimestamp - 2),
      reviewTrigger("game-support-2", "Mixed", "Mostly Positive", baseContext.evaluationTimestamp - 3),
      relevanceTrigger("game-support-3", 0.42, 0.61, baseContext.evaluationTimestamp - 4),
      statusTrigger("game-support-4", "price_lock_ending", baseContext.evaluationTimestamp - 5),
      releaseTrigger("game-support-5", "major_patch", baseContext.evaluationTimestamp - 6),
      reviewTrigger("game-support-6", "Mixed", "Mostly Positive", baseContext.evaluationTimestamp - 7),
    ],
    baseContext,
  );

  assert.equal(result.output.primaryInsight?.gameId, "game-primary");
  assert.equal(result.output.supportingSignals.length, 5);
  assert.equal(result.output.supportingSignals.some((signal) => signal.gameId === "game-support-3"), false);
  assert.equal(result.debug.some((record) => record.discardReason === DISCARD_REASON.OVERFLOW), true);
  assert.equal(result.debug.every((record) => record.priority === DEBUG_PRIORITY.DISCARDED || record.discardReason === null), true);
});
