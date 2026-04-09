import assert from "node:assert/strict";
import test from "node:test";

import { DEBUG_PRIORITY, DISCARD_REASON, TRIGGER_TYPE } from "./constants";
import {
  classifyEvaluationPriority,
  composeEvaluationScore,
  evaluateTrigger,
  getHistoricalContextScore,
  getPriceChangeMagnitude,
  getRecencyScore,
  getReleaseEventMagnitude,
  getRelevanceIncreaseMagnitude,
  getReviewChangeMagnitude,
  getStatusChangeMagnitude,
  getUserRelevanceScore,
  type EvaluationContext,
} from "./evaluation";
import type {
  PriceChangeTrigger,
  ReleaseEventTrigger,
  RelevanceIncreaseTrigger,
  ReviewChangeTrigger,
  StatusChangeTrigger,
} from "./types";

const DAY_MS = 24 * 60 * 60 * 1000;

function assertClose(actual: number, expected: number, epsilon = 1e-9) {
  assert.ok(Math.abs(actual - expected) <= epsilon, `expected ${actual} to be within ${epsilon} of ${expected}`);
}

const baseContext: EvaluationContext = {
  evaluationTimestamp: 2_000_000_000_000,
  userSignals: {
    isDismissed: false,
    isWishlisted: false,
    isViewedOrTracked: false,
    tasteMatch: "none",
  },
  historicalContext: {
    priceContext: "normal",
  },
};

function priceTrigger(previous: number, current: number, timestamp = baseContext.evaluationTimestamp): PriceChangeTrigger {
  return {
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "price-game",
    timestamp,
    previous,
    current,
  };
}

function reviewTrigger(previous: ReviewChangeTrigger["previous"], current: ReviewChangeTrigger["current"]): ReviewChangeTrigger {
  return {
    type: TRIGGER_TYPE.REVIEW_CHANGE,
    gameId: "review-game",
    timestamp: baseContext.evaluationTimestamp,
    previous,
    current,
  };
}

function releaseTrigger(current: ReleaseEventTrigger["current"]): ReleaseEventTrigger {
  return {
    type: TRIGGER_TYPE.RELEASE_EVENT,
    gameId: "release-game",
    timestamp: baseContext.evaluationTimestamp,
    previous: null,
    current,
  };
}

function relevanceTrigger(previous: number, current: number, timestamp = baseContext.evaluationTimestamp): RelevanceIncreaseTrigger {
  return {
    type: TRIGGER_TYPE.RELEVANCE_INCREASE,
    gameId: "relevance-game",
    timestamp,
    previous,
    current,
  };
}

function statusTrigger(current: StatusChangeTrigger["current"]): StatusChangeTrigger {
  return {
    type: TRIGGER_TYPE.STATUS_CHANGE,
    gameId: "status-game",
    timestamp: baseContext.evaluationTimestamp,
    previous: "stable",
    current,
  };
}

test("getPriceChangeMagnitude follows all price drop boundaries", () => {
  assert.equal(getPriceChangeMagnitude(priceTrigger(100, 91)), 0.0);
  assert.equal(getPriceChangeMagnitude(priceTrigger(100, 90)), 0.3);
  assert.equal(getPriceChangeMagnitude(priceTrigger(100, 80)), 0.5);
  assert.equal(getPriceChangeMagnitude(priceTrigger(100, 70)), 0.7);
  assert.equal(getPriceChangeMagnitude(priceTrigger(100, 50)), 1.0);
});

test("getReviewChangeMagnitude follows tier movement rules", () => {
  assert.equal(getReviewChangeMagnitude(reviewTrigger("Mixed", "Mixed")), 0.0);
  assert.equal(getReviewChangeMagnitude(reviewTrigger("Mixed", "Mostly Positive")), 0.5);
  assert.equal(getReviewChangeMagnitude(reviewTrigger("Mostly Negative", "Mostly Positive")), 0.8);
  assert.equal(getReviewChangeMagnitude(reviewTrigger("Mostly Negative", "Overwhelmingly Positive")), 1.0);
});

test("getReleaseEventMagnitude follows release event mapping", () => {
  assert.equal(getReleaseEventMagnitude(releaseTrigger("early_access")), 0.4);
  assert.equal(getReleaseEventMagnitude(releaseTrigger("major_patch")), 0.6);
  assert.equal(getReleaseEventMagnitude(releaseTrigger("released")), 0.8);
});

test("getRelevanceIncreaseMagnitude follows delta boundaries", () => {
  assert.equal(getRelevanceIncreaseMagnitude(relevanceTrigger(0.0, 0.09)), 0.0);
  assert.equal(getRelevanceIncreaseMagnitude(relevanceTrigger(0.0, 0.10)), 0.4);
  assert.equal(getRelevanceIncreaseMagnitude(relevanceTrigger(0.0, 0.19)), 0.4);
  assert.equal(getRelevanceIncreaseMagnitude(relevanceTrigger(0.0, 0.20)), 0.7);
  assert.equal(getRelevanceIncreaseMagnitude(relevanceTrigger(0.0, 0.39)), 0.7);
  assert.equal(getRelevanceIncreaseMagnitude(relevanceTrigger(0.0, 0.40)), 1.0);
});

test("getStatusChangeMagnitude follows status mapping", () => {
  assert.equal(getStatusChangeMagnitude(statusTrigger("price_lock_ending")), 0.6);
  assert.equal(getStatusChangeMagnitude(statusTrigger("leaving_subscription")), 0.8);
  assert.equal(getStatusChangeMagnitude(statusTrigger("delisting")), 1.0);
});

test("getRecencyScore uses exact half-open millisecond intervals", () => {
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - DAY_MS + 1), baseContext.evaluationTimestamp), 1.0);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - DAY_MS), baseContext.evaluationTimestamp), 0.8);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - 4 * DAY_MS + 1), baseContext.evaluationTimestamp), 0.8);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - 4 * DAY_MS), baseContext.evaluationTimestamp), 0.5);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - 8 * DAY_MS + 1), baseContext.evaluationTimestamp), 0.5);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - 8 * DAY_MS), baseContext.evaluationTimestamp), 0.2);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - 15 * DAY_MS + 1), baseContext.evaluationTimestamp), 0.2);
  assert.equal(getRecencyScore(priceTrigger(100, 70, baseContext.evaluationTimestamp - 15 * DAY_MS), baseContext.evaluationTimestamp), 0.0);
});

test("evaluateTrigger discards dismissed users before scoring", () => {
  const result = evaluateTrigger(priceTrigger(100, 50), {
    ...baseContext,
    userSignals: {
      ...baseContext.userSignals,
      isDismissed: true,
    },
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.DISMISSED_BY_USER);
  assert.equal(result.debug.magnitude, null);
  assert.equal(result.debug.recency, null);
  assert.equal(result.debug.user_relevance, null);
  assert.equal(result.debug.historical_context, null);
  assert.equal(result.debug.score, null);
});

test("getUserRelevanceScore handles the single tasteMatch union values and stacked positive signals", () => {
  assert.equal(getUserRelevanceScore({ ...baseContext.userSignals, tasteMatch: "none" }), 0);
  assert.equal(
    getUserRelevanceScore({ ...baseContext.userSignals, isWishlisted: true }),
    0.5,
  );
  assert.equal(
    getUserRelevanceScore({ ...baseContext.userSignals, isViewedOrTracked: true }),
    0.2,
  );
  assert.equal(
    getUserRelevanceScore({ ...baseContext.userSignals, tasteMatch: "moderate" }),
    0.15,
  );
  assert.equal(
    getUserRelevanceScore({ ...baseContext.userSignals, tasteMatch: "strong" }),
    0.3,
  );
  assert.equal(
    getUserRelevanceScore({
      ...baseContext.userSignals,
      isWishlisted: true,
      isViewedOrTracked: true,
      tasteMatch: "strong",
    }),
    1.0,
  );
});

test("getHistoricalContextScore uses supplied price context only for price triggers", () => {
  assert.equal(
    getHistoricalContextScore(priceTrigger(100, 50), { priceContext: "all_time_low" }),
    1.0,
  );
  assert.equal(
    getHistoricalContextScore(reviewTrigger("Mixed", "Mostly Positive"), { priceContext: "all_time_low" }),
    0.3,
  );
});

test("composeEvaluationScore follows the finalized weighting model", () => {
  const score = composeEvaluationScore({
    magnitude: 1.0,
    recency: 0.8,
    user_relevance: 0.5,
    historical_context: 0.7,
  });

  assertClose(score, 0.35 + 0.2 + 0.15 + 0.07);
});

test("classifyEvaluationPriority maps score bands to discarded, medium, and high", () => {
  assert.equal(classifyEvaluationPriority(0.39), DEBUG_PRIORITY.DISCARDED);
  assert.equal(classifyEvaluationPriority(0.4), DEBUG_PRIORITY.MEDIUM);
  assert.equal(classifyEvaluationPriority(0.7), DEBUG_PRIORITY.HIGH);
});

test("evaluateTrigger emits numeric score fields for below-threshold candidates", () => {
  const result = evaluateTrigger(priceTrigger(100, 95, baseContext.evaluationTimestamp - 20 * DAY_MS), baseContext);

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.SCORE_BELOW_THRESHOLD);
  assert.equal(result.debug.magnitude, 0.0);
  assert.equal(result.debug.recency, 0.0);
  assert.equal(result.debug.user_relevance, 0.0);
  assert.equal(result.debug.historical_context, 0.3);
  assert.equal(typeof result.debug.score, "number");
  assertClose(result.debug.score ?? -1, 0.03);
});

test("evaluateTrigger returns a medium priority candidate in the supporting range", () => {
  const result = evaluateTrigger(priceTrigger(100, 80, baseContext.evaluationTimestamp - 4 * DAY_MS), {
    ...baseContext,
    userSignals: {
      ...baseContext.userSignals,
      isViewedOrTracked: true,
      tasteMatch: "moderate",
    },
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.candidate.priority, DEBUG_PRIORITY.MEDIUM);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.MEDIUM);
  assert.equal(result.debug.discardReason, null);
  assertClose(result.candidate.score, 0.5 * 0.35 + 0.5 * 0.25 + 0.35 * 0.3 + 0.3 * 0.1);
  assert.equal(result.debug.magnitude, 0.5);
  assert.equal(result.debug.recency, 0.5);
  assert.equal(result.debug.user_relevance, 0.35);
  assert.equal(result.debug.historical_context, 0.3);
});

test("evaluateTrigger returns a high priority candidate in the primary range", () => {
  const result = evaluateTrigger(priceTrigger(100, 50, baseContext.evaluationTimestamp - DAY_MS + 1), {
    ...baseContext,
    userSignals: {
      ...baseContext.userSignals,
      isWishlisted: true,
      isViewedOrTracked: true,
      tasteMatch: "strong",
    },
    historicalContext: {
      priceContext: "all_time_low",
    },
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.candidate.priority, DEBUG_PRIORITY.HIGH);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.HIGH);
  assert.equal(result.debug.discardReason, null);
  assert.equal(result.debug.magnitude, 1.0);
  assert.equal(result.debug.recency, 1.0);
  assert.equal(result.debug.user_relevance, 1.0);
  assert.equal(result.debug.historical_context, 1.0);
  assertClose(result.candidate.score, 1.0);
});
