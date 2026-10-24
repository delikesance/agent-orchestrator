package pipeline

import (
	"regexp"

	"github.com/aoagents/agent-orchestrator/backend/internal/agentlaunch"
)

const redacted = "[REDACTED]"

// tokenPatterns match common credential shapes. The repository has no shared
// pattern-based redactor (agentlaunch.RedactValues only removes configured
// values), so the pipeline adds these on top of it for command output that is
// persisted as gate evidence.
var tokenPatterns = []*regexp.Regexp{
	regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`github_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`sk-[A-Za-z0-9_-]{16,}`),
	regexp.MustCompile(`xox[abprs]-[A-Za-z0-9-]{10,}`),
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	regexp.MustCompile(`eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}`),
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{12,}`),
	regexp.MustCompile(`(?i)(://[^\s:/@]+:)[^\s@/]+(@)`),
}

// assignmentPattern masks the value of KEY=value / KEY: value pairs whose key
// names a secret.
var assignmentPattern = regexp.MustCompile(`(?i)\b([A-Za-z0-9_.-]*(?:token|secret|password|passwd|api[_-]?key|auth|credential)[A-Za-z0-9_.-]*)(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s"']+)`)

// redact removes configured values (the project's env) and common token
// patterns from s.
func redact(s string, configured map[string]string) string {
	s = agentlaunch.RedactValues(s, configured)
	for _, p := range tokenPatterns {
		if p.NumSubexp() == 0 {
			s = p.ReplaceAllString(s, redacted)
			continue
		}
		s = p.ReplaceAllString(s, "${1}"+redacted+"${2}")
	}
	return assignmentPattern.ReplaceAllString(s, "${1}${2}"+redacted)
}
