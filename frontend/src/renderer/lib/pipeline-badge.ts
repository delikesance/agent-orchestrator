import type { SessionPipeline } from "../types/workspace";

export type PipelineStageName = SessionPipeline["stage"];

export type PipelineBadgeModel = {
	stage: PipelineStageName;
	escalated: boolean;
	/** `2/3`, shown only once a gate attempt has been spent. */
	attemptLabel?: string;
	attempt: number;
	maxAttempts: number;
	/** Evidence summary of the most recent failing gate, when it has one. */
	failureSummary?: string;
};

/**
 * Badge facts for a session's pipeline block, or undefined for a legacy
 * session. The daemon already placed the session on its lane with the stage as
 * a ceiling; this only describes the stage and never picks a lane.
 */
export function pipelineBadgeModel(pipeline: SessionPipeline | undefined): PipelineBadgeModel | undefined {
	if (!pipeline?.stage) return undefined;
	const attempt = pipeline.attempt > 0 ? pipeline.attempt : 0;
	const maxAttempts = pipeline.maxAttempts > 0 ? pipeline.maxAttempts : 0;
	const failing = [...(pipeline.gates ?? [])]
		.filter((gate) => gate.result === "fail")
		.sort((a, b) => (Date.parse(a.createdAt) || 0) - (Date.parse(b.createdAt) || 0))
		.at(-1);
	return {
		stage: pipeline.stage,
		escalated: pipeline.stage === "escalated",
		attemptLabel: attempt > 0 && maxAttempts > 0 ? `${attempt}/${maxAttempts}` : undefined,
		attempt,
		maxAttempts,
		failureSummary: failing?.evidenceSummary?.trim() || undefined,
	};
}
