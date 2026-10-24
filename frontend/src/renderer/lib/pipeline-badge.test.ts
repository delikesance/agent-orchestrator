import { describe, expect, it } from "vitest";
import { pipelineBadgeModel } from "./pipeline-badge";
import type { SessionPipeline } from "../types/workspace";

const gate = (over: Partial<SessionPipeline["gates"][number]>): SessionPipeline["gates"][number] => ({
	stage: "validating",
	attempt: 1,
	result: "fail",
	headSha: "abc",
	createdAt: "2026-01-01T00:00:00Z",
	...over,
});

describe("pipelineBadgeModel", () => {
	it("is absent for a legacy session", () => {
		expect(pipelineBadgeModel(undefined)).toBeUndefined();
	});

	it("omits the attempt until one is spent", () => {
		const model = pipelineBadgeModel({ stage: "building", attempt: 0, maxAttempts: 3, enforce: "on", gates: [] });
		expect(model).toMatchObject({ stage: "building", escalated: false, attemptLabel: undefined });
	});

	it("shows attempt as n/max and the latest failing evidence", () => {
		const model = pipelineBadgeModel({
			stage: "validating",
			attempt: 2,
			maxAttempts: 3,
			enforce: "on",
			gates: [
				gate({ createdAt: "2026-01-02T00:00:00Z", evidenceSummary: "second failure" }),
				gate({ createdAt: "2026-01-01T00:00:00Z", evidenceSummary: "first failure" }),
				gate({ createdAt: "2026-01-03T00:00:00Z", result: "pass", evidenceSummary: "ok" }),
			],
		});
		expect(model?.attemptLabel).toBe("2/3");
		expect(model?.failureSummary).toBe("second failure");
	});

	it("marks escalated", () => {
		const model = pipelineBadgeModel({ stage: "escalated", attempt: 3, maxAttempts: 3, enforce: "on", gates: [] });
		expect(model).toMatchObject({ escalated: true, attemptLabel: "3/3" });
	});
});
