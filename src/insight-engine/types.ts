import { TRIGGER_TYPE } from "./constants";
import type {
  DebugPriority,
  NullableDiscardReason,
  ReleaseEventValue,
  ReviewTier,
  StatusChangeCurrentValue,
  StatusChangePreviousValue,
  TriggerType,
} from "./constants";

export type {
  DebugPriority,
  DiscardReason,
  InsightAction,
  NullableDiscardReason,
  ReleaseEventValue,
  ReviewTier,
  StatusChangeCurrentValue,
  StatusChangePreviousValue,
  StatusChangeValue,
  TriggerType,
} from "./constants";

export type BaseInsightTrigger<TType extends TriggerType = TriggerType> = {
  type: TType;
  gameId: string;
  timestamp: number;
};

export type PriceChangeTrigger = BaseInsightTrigger<typeof TRIGGER_TYPE.PRICE_CHANGE> & {
  previous: number;
  current: number;
};

export type ReviewChangeTrigger = BaseInsightTrigger<typeof TRIGGER_TYPE.REVIEW_CHANGE> & {
  previous: ReviewTier;
  current: ReviewTier;
};

export type ReleaseEventTrigger = BaseInsightTrigger<typeof TRIGGER_TYPE.RELEASE_EVENT> & {
  previous: null;
  current: ReleaseEventValue;
};

export type RelevanceIncreaseTrigger = BaseInsightTrigger<typeof TRIGGER_TYPE.RELEVANCE_INCREASE> & {
  previous: number;
  current: number;
};

export type StatusChangeTrigger = BaseInsightTrigger<typeof TRIGGER_TYPE.STATUS_CHANGE> & {
  previous: StatusChangePreviousValue;
  current: StatusChangeCurrentValue;
};

export type InsightTrigger =
  | PriceChangeTrigger
  | ReviewChangeTrigger
  | ReleaseEventTrigger
  | RelevanceIncreaseTrigger
  | StatusChangeTrigger;

export type Insight = {
  type: TriggerType;
  gameId: string;
  title: string;
  reason: string;
  action: string;
  score: number;
};

export type InsightEngineOutput = {
  primaryInsight: Insight | null;
  supportingSignals: Insight[];
};

export type ScoreComponents = {
  magnitude: number;
  recency: number;
  user_relevance: number;
  historical_context: number;
  score: number;
};

export type DebugRecord = {
  gameId: string;
  type: TriggerType | null;
  magnitude: number | null;
  recency: number | null;
  user_relevance: number | null;
  historical_context: number | null;
  score: number | null;
  priority: DebugPriority;
  discardReason: NullableDiscardReason;
};

export type ValidatedTrigger = InsightTrigger;

export type ScoredCandidate = {
  trigger: InsightTrigger;
  magnitude: number;
  recency: number;
  user_relevance: number;
  historical_context: number;
  score: number;
  priority: DebugPriority;
};

export type TransformedCandidate = {
  trigger: InsightTrigger;
  insight: Insight;
  magnitude: number;
  recency: number;
  user_relevance: number;
  historical_context: number;
  score: number;
  priority: DebugPriority;
};

export type TransformationResult =
  | {
      ok: true;
      candidate: TransformedCandidate;
      debug: DebugRecord;
    }
  | {
      ok: false;
      debug: DebugRecord;
    };

export type FilteringResult = {
  survivors: TransformedCandidate[];
  discarded: DebugRecord[];
};

export type DuplicateResolutionResult = FilteringResult;

export type ConflictResolutionResult = FilteringResult;
