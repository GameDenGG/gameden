import { DEBUG_PRIORITY } from "./constants";
import { filterCandidates } from "./filtering";
import { selectFinalInsights, type FinalSelectionResult } from "./final-selection";
import { evaluateTrigger, type EvaluationContext } from "./evaluation";
import { transformCandidate } from "./transformation";
import { validateTrigger } from "./validation";
import type { DebugRecord, InsightEngineOutput, ScoredCandidate, TransformedCandidate, ValidatedTrigger } from "./types";

export type InsightEngineRunResult = {
  output: InsightEngineOutput;
  debug: DebugRecord[];
};

export function runInsightEngine(triggers: unknown[], context: EvaluationContext): InsightEngineRunResult {
  const debug: DebugRecord[] = [];
  const validatedTriggers: ValidatedTrigger[] = [];

  for (const trigger of triggers) {
    const validationResult = validateTrigger(trigger);
    debug.push(validationResult.debug);
    if (validationResult.ok) {
      validatedTriggers.push(validationResult.trigger);
    }
  }

  const evaluatedCandidates: ScoredCandidate[] = [];

  for (const trigger of validatedTriggers) {
    const evaluationResult = evaluateTrigger(trigger, context);
    debug.push(evaluationResult.debug);
    if (evaluationResult.ok) {
      evaluatedCandidates.push(evaluationResult.candidate);
    }
  }

  const transformedCandidates: TransformedCandidate[] = [];

  for (const candidate of evaluatedCandidates) {
    if (candidate.priority !== DEBUG_PRIORITY.MEDIUM && candidate.priority !== DEBUG_PRIORITY.HIGH) {
      continue;
    }

    const transformationResult = transformCandidate(candidate);
    debug.push(transformationResult.debug);
    if (transformationResult.ok) {
      transformedCandidates.push(transformationResult.candidate);
    }
  }

  const filteredResult = filterCandidates(transformedCandidates);
  debug.push(...filteredResult.discarded);

  const finalSelectionResult: FinalSelectionResult = selectFinalInsights(filteredResult.survivors);
  debug.push(...finalSelectionResult.discarded);

  return {
    output: finalSelectionResult.output,
    debug,
  };
}
