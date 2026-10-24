import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TooltipProvider } from "./ui/tooltip";
import { PipelineStageBadge } from "./PipelineStageBadge";
import type { SessionPipeline } from "../types/workspace";

const base: SessionPipeline = { stage: "validating", attempt: 0, maxAttempts: 3, enforce: "on", gates: [] };
const renderBadge = (pipeline: SessionPipeline | undefined) =>
	render(
		<TooltipProvider>
			<PipelineStageBadge pipeline={pipeline} />
		</TooltipProvider>,
	);

describe("PipelineStageBadge", () => {
	it("renders nothing for a legacy session", () => {
		const { container } = renderBadge(undefined);
		expect(container).toBeEmptyDOMElement();
	});

	it("shows the stage without an attempt before any is spent", () => {
		renderBadge(base);
		expect(screen.getByRole("status")).toHaveTextContent("Validating");
		expect(screen.getByRole("status")).not.toHaveTextContent("/");
	});

	it("shows the attempt as 2/3", () => {
		renderBadge({ ...base, attempt: 2 });
		expect(screen.getByRole("status")).toHaveTextContent("Validating2/3");
		expect(screen.getByLabelText("Pipeline stage: Validating, attempt 2 of 3")).toBeInTheDocument();
	});

	it("falls back to the raw stage for an unknown future stage", () => {
		renderBadge({ ...base, stage: "archiving" as SessionPipeline["stage"] });
		expect(screen.getByRole("status")).toHaveTextContent("archiving");
		expect(screen.getByRole("status")).not.toHaveTextContent("shell.pipelineStage");
		expect(screen.getByLabelText("Pipeline stage: archiving")).toBeInTheDocument();
	});

	it("shows an escalated badge with an accessible label and a focusable evidence tooltip", () => {
		renderBadge({
			...base,
			stage: "escalated",
			attempt: 3,
			gates: [{ stage: "validating", attempt: 3, result: "fail", headSha: "x", createdAt: "2026-01-01T00:00:00Z", evidenceSummary: "go test failed" }],
		});
		const badge = screen.getByLabelText("Pipeline escalated after 3 of 3 attempts");
		expect(badge).toHaveAttribute("data-pipeline-stage", "escalated");
		expect(badge).toHaveAttribute("tabindex", "0");
	});
});
