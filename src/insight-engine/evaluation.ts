import {
  DEBUG_PRIORITY,
  DISCARD_REASON,
  PRIMARY_MIN_SCORE,
  RELEASE_EVENT_VALUE,
  REVIEW_TIERS,
  STATUS_CHANGE_VALUE,
  SUPPORTING_MIN_SCORE,
  TRIGGER_TYPE,
} from "./constants";
import type {
  DebugPriority,
  DebugRecord,
  PriceChangeTrigger,
  ReleaseEventTrigger,
  RelevanceIncreaseTrigger,
  ReviewChangeTrigger,
  ScoredCandidate,
  StatusChangeTrigger,
  ValidatedTrigger,
} from "./types";

const DAY_MS = 24 * 60 * 60 * 1000;

export type EvaluationContext = {
  evaluationTimestamp: number;
  userSignals: {
    isDismissed: boolean;
    isWishlisted: boolean;
    isViewedOrTracked: boolean;
    tasteMatch: "none" | "moderate" | "strong";
  };
  historicalContext: {
    priceContext: "normal" | "lowest_6m" | "lowest_12m" | "all_time_low";
  };
};

export type EvaluationResult =
  | {
      ok: true;
      candidate: ScoredCandidate;
      debug: DebugRecord;
    }
  | {
      ok: false;
      debug: DebugRecord;
    };

type ScoreParts = Pick<ScoredCandidate, "magnitude" | "recency" | "user_relevance" | "historical_context">;

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function failInvalidState(message: string): never {
  throw new Error(message);
}

function getTasteMatchScore(tasteMatch: EvaluationContext["userSignals"]["tasteMatch"]): number {
  if (tasteMatch === "strong") {
    return 0.3;
  }
  if (tasteMatch === "moderate") {
    return 0.15;
  }
  return 0;
}

function buildDebugRecord(
  trigger: ValidatedTrigger,
  priority: DebugPriority,
  discardReason: DebugRecord["discardReason"],
  parts?: (ScoreParts & { score: number }) | null,
): DebugRecord {
  return {
    gameId: trigger.gameId,
    type: trigger.type,
    magnitude: parts?.magnitude ?? null,
    recency: parts?.recency ?? null,
    user_relevance: parts?.user_relevance ?? null,
    historical_context: parts?.historical_context ?? null,
    score: parts?.score ?? null,
    priority,
    discardReason,
  };
}

export function getPriceChangeMagnitude(trigger: PriceChangeTrigger): number {
  const drop = (trigger.previous - trigger.current) / trigger.previous;

  if (drop < 0.1) {
    return 0.0;
  }
  if (drop < 0.2) {
    return 0.3;
  }
  if (drop < 0.3) {
    return 0.5;
  }
  if (drop < 0.5) {
    return 0.7;
  }
  if (drop >= 0.5) {
    return 1.0;
  }
  return 0.0;
}

export function getReviewChangeMagnitude(trigger: ReviewChangeTrigger): number {
  const previousIndex = REVIEW_TIERS.indexOf(trigger.previous);
  const currentIndex = REVIEW_TIERS.indexOf(trigger.current);
  const delta = currentIndex - previousIndex;

  if (delta <= 0) {
    return 0.0;
  }
  if (delta === 1) {
    return 0.5;
  }
  if (delta === 2) {
    return 0.8;
  }
  return 1.0;
}

export function getReleaseEventMagnitude(trigger: ReleaseEventTrigger): number {
  switch (trigger.current) {
    case RELEASE_EVENT_VALUE.EARLY_ACCESS:
      return 0.4;
    case RELEASE_EVENT_VALUE.MAJOR_PATCH:
      return 0.6;
    case RELEASE_EVENT_VALUE.RELEASED:
      return 0.8;
  }

  return failInvalidState(`Unhandled release event value: ${String(trigger.current)}`);
}

export function getRelevanceIncreaseMagnitude(trigger: RelevanceIncreaseTrigger): number {
  const delta = trigger.current - trigger.previous;

  if (delta < 0.1) {
    return 0.0;
  }
  if (delta < 0.2) {
    return 0.4;
  }
  if (delta < 0.4) {
    return 0.7;
  }
  return 1.0;
}

export function getStatusChangeMagnitude(trigger: StatusChangeTrigger): number {
  switch (trigger.current) {
    case STATUS_CHANGE_VALUE.PRICE_LOCK_ENDING:
      return 0.6;
    case STATUS_CHANGE_VALUE.LEAVING_SUBSCRIPTION:
      return 0.8;
    case STATUS_CHANGE_VALUE.DELISTING:
      return 1.0;
  }

  return failInvalidState(`Unhandled status change value: ${String(trigger.current)}`);
}

export function getRecencyScore(trigger: ValidatedTrigger, evaluationTimestamp: number): number {
  const ageMs = evaluationTimestamp - trigger.timestamp;

  if (ageMs < DAY_MS) {
    return 1.0;
  }
  if (ageMs < 4 * DAY_MS) {
    return 0.8;
  }
  if (ageMs < 8 * DAY_MS) {
    return 0.5;
  }
  if (ageMs < 15 * DAY_MS) {
    return 0.2;
  }
  return 0.0;
}

export function getUserRelevanceScore(context: EvaluationContext["userSignals"]): number {
  let total = 0;

  if (context.isDismissed) {
    return 0;
  }
  if (context.isWishlisted) {
    total += 0.5;
  }
  if (context.isViewedOrTracked) {
    total += 0.2;
  }

  total += getTasteMatchScore(context.tasteMatch);
  return clamp(total, 0, 1);
}

export function getHistoricalContextScore(
  trigger: ValidatedTrigger,
  context: EvaluationContext["historicalContext"],
): number {
  if (trigger.type !== TRIGGER_TYPE.PRICE_CHANGE) {
    return 0.3;
  }

  switch (context.priceContext) {
    case "normal":
      return 0.3;
    case "lowest_6m":
      return 0.7;
    case "lowest_12m":
      return 0.9;
    case "all_time_low":
      return 1.0;
  }

  return failInvalidState(`Unhandled price context value: ${String(context.priceContext)}`);
}

export function composeEvaluationScore(parts: ScoreParts): number {
  return (
    parts.magnitude * 0.35 +
    parts.recency * 0.25 +
    parts.user_relevance * 0.3 +
    parts.historical_context * 0.1
  );
}

export function classifyEvaluationPriority(score: number): DebugPriority {
  if (score < SUPPORTING_MIN_SCORE) {
    return DEBUG_PRIORITY.DISCARDED;
  }
  if (score < PRIMARY_MIN_SCORE) {
    return DEBUG_PRIORITY.MEDIUM;
  }
  return DEBUG_PRIORITY.HIGH;
}

function getMagnitude(trigger: ValidatedTrigger): number {
  switch (trigger.type) {
    case TRIGGER_TYPE.PRICE_CHANGE:
      return getPriceChangeMagnitude(trigger);
    case TRIGGER_TYPE.REVIEW_CHANGE:
      return getReviewChangeMagnitude(trigger);
    case TRIGGER_TYPE.RELEASE_EVENT:
      return getReleaseEventMagnitude(trigger);
    case TRIGGER_TYPE.RELEVANCE_INCREASE:
      return getRelevanceIncreaseMagnitude(trigger);
    case TRIGGER_TYPE.STATUS_CHANGE:
      return getStatusChangeMagnitude(trigger);
  }

  return failInvalidState(`Unhandled trigger type during evaluation: ${String(trigger.type)}`);
}

export function evaluateTrigger(trigger: ValidatedTrigger, context: EvaluationContext): EvaluationResult {
  if (context.userSignals.isDismissed) {
    return {
      ok: false,
      debug: buildDebugRecord(trigger, DEBUG_PRIORITY.DISCARDED, DISCARD_REASON.DISMISSED_BY_USER),
    };
  }

  const parts: ScoreParts = {
    magnitude: getMagnitude(trigger),
    recency: getRecencyScore(trigger, context.evaluationTimestamp),
    user_relevance: getUserRelevanceScore(context.userSignals),
    historical_context: getHistoricalContextScore(trigger, context.historicalContext),
  };
  const score = composeEvaluationScore(parts);
  const priority = classifyEvaluationPriority(score);
  const debug = buildDebugRecord(
    trigger,
    priority,
    priority === DEBUG_PRIORITY.DISCARDED ? DISCARD_REASON.SCORE_BELOW_THRESHOLD : null,
    { ...parts, score },
  );

  if (priority === DEBUG_PRIORITY.DISCARDED) {
    return {
      ok: false,
      debug,
    };
  }

  return {
    ok: true,
    candidate: {
      trigger,
      ...parts,
      score,
      priority,
    },
    debug,
  };
}
