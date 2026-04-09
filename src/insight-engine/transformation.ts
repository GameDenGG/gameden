import {
  DEBUG_PRIORITY,
  DISCARD_REASON,
  INSIGHT_ACTION,
  RELEASE_EVENT_VALUE,
  STATUS_CHANGE_VALUE,
  TRIGGER_TYPE,
} from "./constants";
import type {
  DebugRecord,
  Insight,
  NullableDiscardReason,
  ScoredCandidate,
  TransformedCandidate,
  TransformationResult,
} from "./types";

export type { TransformationResult } from "./types";

const TITLE_MAX_LENGTH = 50;
const REASON_MAX_LENGTH = 120;
const ACTION_MAX_LENGTH = 30;

function buildDebugRecord(
  candidate: ScoredCandidate,
  priority: DebugRecord["priority"],
  discardReason: NullableDiscardReason,
): DebugRecord {
  return {
    gameId: candidate.trigger.gameId,
    type: candidate.trigger.type,
    magnitude: candidate.magnitude,
    recency: candidate.recency,
    user_relevance: candidate.user_relevance,
    historical_context: candidate.historical_context,
    score: candidate.score,
    priority,
    discardReason,
  };
}

function finishInsight(insight: Insight): Insight {
  return insight;
}

export function validateInsightOutputConstraints(insight: Insight): boolean {
  return (
    insight.title.length <= TITLE_MAX_LENGTH &&
    insight.reason.length > 0 &&
    insight.reason.length <= REASON_MAX_LENGTH &&
    insight.action.length <= ACTION_MAX_LENGTH
  );
}

export function transformPriceChangeCandidate(candidate: ScoredCandidate): Insight {
  const trigger = candidate.trigger;
  return finishInsight({
    type: trigger.type,
    gameId: trigger.gameId,
    title: `Buy now at $${trigger.current} after $${trigger.previous} drop`,
    reason: `Price fell from $${trigger.previous} to $${trigger.current}`,
    action: INSIGHT_ACTION.BUY_NOW,
    score: candidate.score,
  });
}

export function transformReviewChangeCandidate(candidate: ScoredCandidate): Insight {
  const trigger = candidate.trigger;
  return finishInsight({
    type: trigger.type,
    gameId: trigger.gameId,
    title: `Reviews: ${trigger.previous} → ${trigger.current}`,
    reason: `Shifted from ${trigger.previous} to ${trigger.current}`,
    action: INSIGHT_ACTION.WAIT,
    score: candidate.score,
  });
}

export function transformReleaseEventCandidate(candidate: ScoredCandidate): Insight {
  const trigger = candidate.trigger;

  switch (trigger.current) {
    case RELEASE_EVENT_VALUE.RELEASED:
      return finishInsight({
        type: trigger.type,
        gameId: trigger.gameId,
        title: "Now fully released",
        reason: "Moved from pre-release to released",
        action: INSIGHT_ACTION.WAIT,
        score: candidate.score,
      });
    case RELEASE_EVENT_VALUE.EARLY_ACCESS:
      return finishInsight({
        type: trigger.type,
        gameId: trigger.gameId,
        title: "Now in early access",
        reason: "Entered early access",
        action: INSIGHT_ACTION.WAIT,
        score: candidate.score,
      });
    case RELEASE_EVENT_VALUE.MAJOR_PATCH:
      return finishInsight({
        type: trigger.type,
        gameId: trigger.gameId,
        title: "Major patch landed",
        reason: "A major update was released",
        action: INSIGHT_ACTION.WAIT,
        score: candidate.score,
      });
  }

  throw new Error(`Unhandled release event value: ${String(trigger.current)}`);
}

export function transformRelevanceIncreaseCandidate(candidate: ScoredCandidate): Insight {
  const trigger = candidate.trigger;
  return finishInsight({
    type: trigger.type,
    gameId: trigger.gameId,
    title: "New strong match for you",
    reason: "Relevance crossed the attention threshold",
    action: INSIGHT_ACTION.WAIT,
    score: candidate.score,
  });
}

export function transformStatusChangeCandidate(candidate: ScoredCandidate): Insight {
  const trigger = candidate.trigger;

  switch (trigger.current) {
    case STATUS_CHANGE_VALUE.DELISTING:
      return finishInsight({
        type: trigger.type,
        gameId: trigger.gameId,
        title: "Delisting soon",
        reason: "This game is heading for delisting",
        action: INSIGHT_ACTION.BUY_NOW,
        score: candidate.score,
      });
    case STATUS_CHANGE_VALUE.LEAVING_SUBSCRIPTION:
      return finishInsight({
        type: trigger.type,
        gameId: trigger.gameId,
        title: "Leaving subscription soon",
        reason: "Access is ending in subscription",
        action: INSIGHT_ACTION.BUY_NOW,
        score: candidate.score,
      });
    case STATUS_CHANGE_VALUE.PRICE_LOCK_ENDING:
      return finishInsight({
        type: trigger.type,
        gameId: trigger.gameId,
        title: "Price lock ending soon",
        reason: "Locked pricing is about to end",
        action: INSIGHT_ACTION.BUY_NOW,
        score: candidate.score,
      });
  }

  throw new Error(`Unhandled status change value: ${String(trigger.current)}`);
}

function transformInsight(candidate: ScoredCandidate): Insight {
  switch (candidate.trigger.type) {
    case TRIGGER_TYPE.PRICE_CHANGE:
      return transformPriceChangeCandidate(candidate);
    case TRIGGER_TYPE.REVIEW_CHANGE:
      return transformReviewChangeCandidate(candidate);
    case TRIGGER_TYPE.RELEASE_EVENT:
      return transformReleaseEventCandidate(candidate);
    case TRIGGER_TYPE.RELEVANCE_INCREASE:
      return transformRelevanceIncreaseCandidate(candidate);
    case TRIGGER_TYPE.STATUS_CHANGE:
      return transformStatusChangeCandidate(candidate);
  }

  throw new Error(`Unhandled trigger type during transformation: ${String(candidate.trigger.type)}`);
}

function buildTransformedCandidate(candidate: ScoredCandidate, insight: Insight): TransformedCandidate {
  return {
    trigger: candidate.trigger,
    insight,
    magnitude: candidate.magnitude,
    recency: candidate.recency,
    user_relevance: candidate.user_relevance,
    historical_context: candidate.historical_context,
    score: candidate.score,
    priority: candidate.priority,
  };
}

export function transformCandidate(candidate: ScoredCandidate): TransformationResult {
  const insight = transformInsight(candidate);
  const debug = buildDebugRecord(candidate, candidate.priority, null);

  if (!validateInsightOutputConstraints(insight)) {
    return {
      ok: false,
      debug: buildDebugRecord(candidate, DEBUG_PRIORITY.DISCARDED, DISCARD_REASON.OVERFLOW),
    };
  }

  return {
    ok: true,
    candidate: buildTransformedCandidate(candidate, insight),
    debug,
  };
}
