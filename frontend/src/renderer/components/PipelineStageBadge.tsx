import { useTranslation } from "react-i18next";
import { pipelineBadgeModel } from "../lib/pipeline-badge";
import type { SessionPipeline } from "../types/workspace";
import { Badge } from "./ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip";

/**
 * Compact pipeline stage badge for a board card. Renders nothing for a legacy
 * session. Escalated is the only colored state (destructive token); the last
 * failing gate's evidence summary is a tooltip, reachable by keyboard focus.
 */
export function PipelineStageBadge({ pipeline }: { pipeline: SessionPipeline | undefined }) {
	const { t } = useTranslation();
	const model = pipelineBadgeModel(pipeline);
	if (!model) return null;
	const stage = t(`shell.pipelineStage.${model.stage}`);
	const label = model.escalated
		? t("shell.pipelineEscalatedAria", { attempt: model.attempt, max: model.maxAttempts })
		: model.attemptLabel
			? t("shell.pipelineStageAttemptAria", { stage, attempt: model.attempt, max: model.maxAttempts })
			: t("shell.pipelineStageAria", { stage });
	const badge = (
		<Badge
			aria-label={label}
			className="tabular-nums"
			data-pipeline-stage={model.stage}
			role="status"
			tabIndex={model.failureSummary ? 0 : undefined}
			variant={model.escalated ? "error" : "neutral"}
		>
			<span>{stage}</span>
			{model.attemptLabel ? <span>{model.attemptLabel}</span> : null}
		</Badge>
	);
	if (!model.failureSummary) return badge;
	return (
		<Tooltip>
			<TooltipTrigger asChild>{badge}</TooltipTrigger>
			<TooltipContent side="bottom">{t("shell.pipelineLastFailure", { summary: model.failureSummary })}</TooltipContent>
		</Tooltip>
	);
}
