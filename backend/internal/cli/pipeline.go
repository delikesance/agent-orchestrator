package cli

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// sessionPipelineDTO mirrors controllers.SessionPipelineView, the pipeline block
// of a session. Nil for legacy sessions.
type sessionPipelineDTO struct {
	Stage       string                `json:"stage"`
	Attempt     int                   `json:"attempt"`
	MaxAttempts int                   `json:"maxAttempts"`
	Enforce     string                `json:"enforce"`
	Gates       []sessionPipelineGate `json:"gates"`
}

// sessionPipelineGate mirrors controllers.SessionPipelineGate.
type sessionPipelineGate struct {
	Stage           string    `json:"stage"`
	Attempt         int       `json:"attempt"`
	Result          string    `json:"result"`
	HeadSha         string    `json:"headSha"`
	ReviewRunID     string    `json:"reviewRunId,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
	EvidenceSummary string    `json:"evidenceSummary,omitempty"`
}

// pipelineStatusResponse mirrors controllers.PipelineStatusResponse.
type pipelineStatusResponse struct {
	SessionID   string               `json:"sessionId"`
	Stage       string               `json:"stage"`
	Attempt     int                  `json:"attempt"`
	MaxAttempts int                  `json:"maxAttempts"`
	Enforce     string               `json:"enforce"`
	Enforced    bool                 `json:"enforced"`
	Blocking    bool                 `json:"blocking"`
	Gates       []pipelineGateDetail `json:"gates"`
}

type pipelineGateDetail struct {
	sessionPipelineGate
	Evidence string `json:"evidence,omitempty"`
}

// pipelineActionResponse mirrors controllers.PipelineActionResponse.
type pipelineActionResponse struct {
	SessionID   string `json:"sessionId"`
	From        string `json:"from"`
	To          string `json:"to"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"maxAttempts"`
}

// pipelineOverrideRequest mirrors controllers.PipelineOverrideRequest.
type pipelineOverrideRequest struct {
	Reason string `json:"reason"`
}

func newPipelineCommand(ctx *commandContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Inspect and steer a session's delivery pipeline",
		Long: `The delivery pipeline moves a session through building, validating, review
and ready before it can merge. These commands show where a session is and
unblock one that escalated.`,
	}
	cmd.AddCommand(
		newPipelineStatusCommand(ctx),
		newPipelineRetryCommand(ctx),
		newPipelineOverrideCommand(ctx),
	)
	return cmd
}

func newPipelineStatusCommand(ctx *commandContext) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "status [id]",
		Short: "Show a session's pipeline stage, attempts and gates (default: this session)",
		Args:  atMostOneArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := pipelineTargetSession(args)
			if err != nil {
				return err
			}
			var res pipelineStatusResponse
			if err := ctx.getJSON(cmd.Context(), "sessions/"+url.PathEscape(id)+"/pipeline", &res); err != nil {
				return err
			}
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), res)
			}
			return writePipelineStatus(cmd.OutOrStdout(), res)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Output as JSON, including each gate's full evidence")
	return cmd
}

func newPipelineRetryCommand(ctx *commandContext) *cobra.Command {
	return &cobra.Command{
		Use:   "retry <id>",
		Short: "Return an escalated session to building with a fresh attempt budget",
		Long: `Retry an escalated session: it goes back to building and its attempt counter
resets. Allowed for a person and for an orchestrator of the session's project.`,
		Args: oneSessionIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := normalizeSessionID(args[0])
			if err != nil {
				return err
			}
			var res pipelineActionResponse
			if err := ctx.postJSON(cmd.Context(), "sessions/"+url.PathEscape(id)+"/pipeline/retry", struct{}{}, &res); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "session %s: pipeline %s -> %s (attempt %d/%d)\n", res.SessionID, res.From, res.To, res.Attempt, res.MaxAttempts)
			return err
		},
	}
}

func newPipelineOverrideCommand(ctx *commandContext) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "override <id> --reason <text>",
		Short: "Accept an escalated session as is and move it to ready (a person only)",
		Long: `Override an escalated session: it moves to ready and a gate recording your
reason is stored on the current head. Only a person may do this; an agent
session is refused by the daemon.`,
		Args: oneSessionIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := normalizeSessionID(args[0])
			if err != nil {
				return err
			}
			reason = strings.TrimSpace(reason)
			if reason == "" {
				return usageError{errors.New("--reason is required")}
			}
			var res pipelineActionResponse
			if err := ctx.postJSON(cmd.Context(), "sessions/"+url.PathEscape(id)+"/pipeline/override", pipelineOverrideRequest{Reason: reason}, &res); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "session %s: pipeline %s -> %s (override recorded)\n", res.SessionID, res.From, res.To)
			return err
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "Why the session is accepted as is (required)")
	return cmd
}

// pipelineTargetSession resolves the positional id, falling back to the calling
// AO session.
func pipelineTargetSession(args []string) (string, error) {
	if len(args) == 1 {
		return normalizeSessionID(args[0])
	}
	if id := strings.TrimSpace(os.Getenv("AO_SESSION_ID")); id != "" {
		return id, nil
	}
	return "", usageError{errors.New("usage: session id is required (positional) outside an AO session")}
}

func writePipelineStatus(out io.Writer, res pipelineStatusResponse) error {
	if res.Stage == "" {
		_, err := fmt.Fprintf(out, "session %s: no pipeline (legacy session)\n", res.SessionID)
		return err
	}
	if _, err := fmt.Fprintf(out, "session: %s\nstage: %s\nattempt: %d/%d\nenforce: %s\n", res.SessionID, res.Stage, res.Attempt, res.MaxAttempts, res.Enforce); err != nil {
		return err
	}
	if res.Blocking {
		if _, err := fmt.Fprintln(out, "merge: blocked until the session is ready"); err != nil {
			return err
		}
	}
	gates := make([]sessionPipelineGate, 0, len(res.Gates))
	for _, g := range res.Gates {
		gates = append(gates, g.sessionPipelineGate)
	}
	return writePipelineGates(out, "", gates)
}

func writePipelineGates(out io.Writer, indent string, gates []sessionPipelineGate) error {
	if len(gates) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(out, "%sgates:\n", indent); err != nil {
		return err
	}
	for _, g := range gates {
		line := fmt.Sprintf("%s  %s #%d %s @ %s", indent, g.Stage, g.Attempt, g.Result, shortReviewSHA(g.HeadSha))
		if g.EvidenceSummary != "" {
			line += " - " + g.EvidenceSummary
		}
		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	return nil
}

// writeSessionPipeline renders the pipeline block of `ao session get`.
func writeSessionPipeline(out io.Writer, p *sessionPipelineDTO) error {
	if p == nil {
		return nil
	}
	if _, err := fmt.Fprintf(out, "pipeline: %s (attempt %d/%d, enforce %s)\n", p.Stage, p.Attempt, p.MaxAttempts, p.Enforce); err != nil {
		return err
	}
	return writePipelineGates(out, "  ", p.Gates)
}
