import assert from "node:assert/strict";
import test from "node:test";

import { DEBUG_PRIORITY, DISCARD_REASON, INSIGHT_ACTION, TRIGGER_TYPE } from "./constants";
import { selectFinalInsights } from "./final-selection";
import type { TransformedCandidate } from "./types";

function makeCandidate(overrides: Partial<TransformedCandidate> & Pick<TransformedCandidate, "trigger" | "insight">): TransformedCandidate {
  return {
    trigger: overrides.trigger,
    insight: overrides.insight,
    magnitude: overrides.magnitude ?? 0.5,
    recency: overrides.recency ?? 0.5,
    user_relevance: overrides.user_relevance ?? 0.5,
    historical_context: overrides.historical_context ?? 0.5,
    score: overrides.score ?? 0.5,
    priority: overrides.priority ?? DEBUG_PRIORITY.MEDIUM,
  };
}

function highPriceCandidate(gameId: string, score: number, userRelevance: number, timestamp: number): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId,
      timestamp,
      previous: 100,
      current: 50,
    },
    insight: {
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId,
      title: "Buy now at $50 after $100 drop",
      reason: "Price fell from $100 to $50",
      action: INSIGHT_ACTION.BUY_NOW,
      score,
    },
    score,
    user_relevance: userRelevance,
    priority: DEBUG_PRIORITY.HIGH,
  });
}

function highStatusCandidate(gameId: string, score: number, userRelevance: number, timestamp: number): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.STATUS_CHANGE,
      gameId,
      timestamp,
      previous: "stable",
      current: "delisting",
    },
    insight: {
      type: TRIGGER_TYPE.STATUS_CHANGE,
      gameId,
      title: "Delisting soon",
      reason: "This game is heading for delisting",
      action: INSIGHT_ACTION.BUY_NOW,
      score,
    },
    score,
    user_relevance: userRelevance,
    priority: DEBUG_PRIORITY.HIGH,
  });
}

function mediumCandidate(
  gameId: string,
  type: TransformedCandidate["trigger"]["type"],
  score: number,
  userRelevance: number,
  timestamp: number,
  action: string,
): TransformedCandidate {
  const insightMap = {
    [TRIGGER_TYPE.PRICE_CHANGE]: {
      title: "Buy now at $75 after $100 drop",
      reason: "Price fell from $100 to $75",
      action,
    },
    [TRIGGER_TYPE.REVIEW_CHANGE]: {
      title: "Reviews: Mixed → Mostly Positive",
      reason: "Shifted from Mixed to Mostly Positive",
      action,
    },
    [TRIGGER_TYPE.RELEASE_EVENT]: {
      title: "Now fully released",
      reason: "Moved from pre-release to released",
      action,
    },
    [TRIGGER_TYPE.RELEVANCE_INCREASE]: {
      title: "New strong match for you",
      reason: "Relevance crossed the attention threshold",
      action,
    },
    [TRIGGER_TYPE.STATUS_CHANGE]: {
      title: "Delisting soon",
      reason: "This game is heading for delisting",
      action,
    },
  } as const;

  const insight = insightMap[type];

  return makeCandidate({
    trigger:
      type === TRIGGER_TYPE.PRICE_CHANGE
        ? { type, gameId, timestamp, previous: 100, current: 75 }
        : type === TRIGGER_TYPE.REVIEW_CHANGE
          ? { type, gameId, timestamp, previous: "Mixed", current: "Mostly Positive" }
          : type === TRIGGER_TYPE.RELEASE_EVENT
            ? { type, gameId, timestamp, previous: null, current: "released" }
            : type === TRIGGER_TYPE.RELEVANCE_INCREASE
              ? { type, gameId, timestamp, previous: 0.42, current: 0.61 }
              : { type, gameId, timestamp, previous: "stable", current: "delisting" },
    insight: {
      type,
      gameId,
      title: insight.title,
      reason: insight.reason,
      action: insight.action,
      score,
    },
    score,
    user_relevance: userRelevance,
    priority: DEBUG_PRIORITY.MEDIUM,
  });
}

test("selectFinalInsights returns a valid null output for empty input", () => {
  const result = selectFinalInsights([]);

  assert.equal(result.output.primaryInsight, null);
  assert.deepEqual(result.output.supportingSignals, []);
  assert.deepEqual(result.discarded, []);
});

test("selectFinalInsights selects one high candidate as primary", () => {
  const high = highPriceCandidate("game-primary", 0.9, 0.4, 10);

  const result = selectFinalInsights([high]);

  assert.deepEqual(result.output.primaryInsight, high.insight);
  assert.deepEqual(result.output.supportingSignals, []);
  assert.deepEqual(result.discarded, []);
});

test("selectFinalInsights leaves primary null when no high candidates exist", () => {
  const medium = mediumCandidate("game-medium", TRIGGER_TYPE.REVIEW_CHANGE, 0.6, 0.4, 10, INSIGHT_ACTION.WAIT);

  const result = selectFinalInsights([medium]);

  assert.equal(result.output.primaryInsight, null);
  assert.deepEqual(result.output.supportingSignals, [medium.insight]);
  assert.deepEqual(result.discarded, []);
});

test("selectFinalInsights keeps only the top-ranked high and discards the rest as overflow", () => {
  const primary = highPriceCandidate("game-high", 0.9, 0.4, 10);
  const loser = highStatusCandidate("game-high", 0.9, 0.4, 10);

  const result = selectFinalInsights([loser, primary]);

  assert.deepEqual(result.output.primaryInsight, primary.insight);
  assert.deepEqual(result.output.supportingSignals, []);
  assert.equal(result.discarded.length, 1);
  assert.equal(result.discarded[0].gameId, "game-high");
  assert.equal(result.discarded[0].type, TRIGGER_TYPE.STATUS_CHANGE);
  assert.equal(result.discarded[0].priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.OVERFLOW);
  assert.equal(result.discarded[0].score, 0.9);
  assert.equal(result.discarded[0].magnitude, loser.magnitude);
  assert.equal(result.discarded[0].recency, loser.recency);
  assert.equal(result.discarded[0].user_relevance, loser.user_relevance);
  assert.equal(result.discarded[0].historical_context, loser.historical_context);
});

test("selectFinalInsights caps supporting signals at five and preserves comparator order", () => {
  const candidates = [
    mediumCandidate("m1", TRIGGER_TYPE.RELEVANCE_INCREASE, 0.1, 0.1, 1, INSIGHT_ACTION.WAIT),
    mediumCandidate("m2", TRIGGER_TYPE.RELEASE_EVENT, 0.9, 0.1, 2, INSIGHT_ACTION.WAIT),
    mediumCandidate("m3", TRIGGER_TYPE.REVIEW_CHANGE, 0.8, 0.1, 3, INSIGHT_ACTION.WAIT),
    mediumCandidate("m4", TRIGGER_TYPE.STATUS_CHANGE, 0.7, 0.1, 4, INSIGHT_ACTION.WAIT),
    mediumCandidate("m5", TRIGGER_TYPE.PRICE_CHANGE, 0.6, 0.1, 5, INSIGHT_ACTION.BUY_NOW),
    mediumCandidate("m6", TRIGGER_TYPE.RELEVANCE_INCREASE, 0.5, 0.1, 6, INSIGHT_ACTION.WAIT),
  ];

  const result = selectFinalInsights(candidates);

  assert.equal(result.output.primaryInsight, null);
  assert.equal(result.output.supportingSignals.length, 5);
  assert.deepEqual(result.output.supportingSignals, [
    candidates[1].insight,
    candidates[2].insight,
    candidates[3].insight,
    candidates[4].insight,
    candidates[5].insight,
  ]);
  assert.equal(result.discarded.length, 1);
  assert.equal(result.discarded[0].gameId, "m1");
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.OVERFLOW);
  assert.equal(result.discarded[0].priority, DEBUG_PRIORITY.DISCARDED);
});

test("selectFinalInsights keeps high primary and fills supporting with mediums without demotion", () => {
  const high = highPriceCandidate("game-mix", 0.1, 0.1, 1);
  const mediums = [
    mediumCandidate("mix-1", TRIGGER_TYPE.RELEASE_EVENT, 0.9, 0.2, 2, INSIGHT_ACTION.WAIT),
    mediumCandidate("mix-2", TRIGGER_TYPE.REVIEW_CHANGE, 0.8, 0.2, 3, INSIGHT_ACTION.WAIT),
    mediumCandidate("mix-3", TRIGGER_TYPE.STATUS_CHANGE, 0.7, 0.2, 4, INSIGHT_ACTION.BUY_NOW),
  ];

  const result = selectFinalInsights([mediums[0], high, mediums[1], mediums[2]]);

  assert.deepEqual(result.output.primaryInsight, high.insight);
  assert.deepEqual(result.output.supportingSignals, [
    mediums[0].insight,
    mediums[1].insight,
    mediums[2].insight,
  ]);
  assert.deepEqual(result.discarded, []);
});

test("selectFinalInsights discards extra medium overflow candidates with numeric fields preserved", () => {
  const candidates = [
    mediumCandidate("m1", TRIGGER_TYPE.PRICE_CHANGE, 0.9, 0.1, 1, INSIGHT_ACTION.BUY_NOW),
    mediumCandidate("m2", TRIGGER_TYPE.STATUS_CHANGE, 0.8, 0.2, 2, INSIGHT_ACTION.BUY_NOW),
    mediumCandidate("m3", TRIGGER_TYPE.REVIEW_CHANGE, 0.7, 0.3, 3, INSIGHT_ACTION.WAIT),
    mediumCandidate("m4", TRIGGER_TYPE.RELEASE_EVENT, 0.6, 0.4, 4, INSIGHT_ACTION.WAIT),
    mediumCandidate("m5", TRIGGER_TYPE.RELEVANCE_INCREASE, 0.5, 0.5, 5, INSIGHT_ACTION.WAIT),
    mediumCandidate("m6", TRIGGER_TYPE.RELEVANCE_INCREASE, 0.4, 0.6, 6, INSIGHT_ACTION.WAIT),
  ];

  const result = selectFinalInsights(candidates);

  assert.equal(result.output.supportingSignals.length, 5);
  assert.equal(result.discarded.length, 1);
  assert.equal(result.discarded[0].gameId, "m6");
  assert.equal(result.discarded[0].type, TRIGGER_TYPE.RELEVANCE_INCREASE);
  assert.equal(result.discarded[0].magnitude, candidates[5].magnitude);
  assert.equal(result.discarded[0].recency, candidates[5].recency);
  assert.equal(result.discarded[0].user_relevance, candidates[5].user_relevance);
  assert.equal(result.discarded[0].historical_context, candidates[5].historical_context);
  assert.equal(result.discarded[0].score, candidates[5].score);
  assert.equal(result.discarded[0].priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.OVERFLOW);
});

test("selectFinalInsights uses the shared comparator for tie-sensitive ordering", () => {
  const review = mediumCandidate("tie-review", TRIGGER_TYPE.REVIEW_CHANGE, 0.7, 0.2, 10, INSIGHT_ACTION.WAIT);
  const release = mediumCandidate("tie-release", TRIGGER_TYPE.RELEASE_EVENT, 0.7, 0.2, 10, INSIGHT_ACTION.WAIT);

  const result = selectFinalInsights([release, review]);

  assert.equal(result.output.primaryInsight, null);
  assert.deepEqual(result.output.supportingSignals, [review.insight, release.insight]);
  assert.deepEqual(result.discarded, []);
});
