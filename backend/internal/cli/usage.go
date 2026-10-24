package cli

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

const usageUnknownGroup = "unknown"

const usageEstimateNote = "Costs are estimates from the model price catalog, not the provider invoice."

type usageOptions struct {
	by      string
	project string
	json    bool
}

// usageCompactDTO mirrors controllers.CompactSessionUsageResponse.
type usageCompactDTO struct {
	SessionID       string `json:"sessionId"`
	ProcessedTokens *int64 `json:"processedTokens"`
	EstimatedCost   *struct {
		TotalNanos int64 `json:"totalNanos"`
	} `json:"estimatedCost"`
}

type usageCompactResponse struct {
	Sessions []usageCompactDTO `json:"sessions"`
}

// usageRow is one grouped line of `ao usage`. Cost is an estimate from the
// model price catalog, never the provider invoice.
type usageRow struct {
	Group               string  `json:"group"`
	Sessions            int     `json:"sessions"`
	SessionsWithUsage   int     `json:"sessionsWithUsage"`
	ProcessedTokens     int64   `json:"processedTokens"`
	EstimatedCostUSD    float64 `json:"estimatedCostUsd"`
	AvgEstimatedCostUSD float64 `json:"avgEstimatedCostUsd"`
	costNanos           int64
	costSessions        int
}

type usageOutput struct {
	By   string     `json:"by"`
	Note string     `json:"note"`
	Data []usageRow `json:"data"`
}

func newUsageCommand(ctx *commandContext) *cobra.Command {
	var opts usageOptions
	cmd := &cobra.Command{
		Use:   "usage",
		Short: "Compare token usage and estimated cost by tier or model",
		Long:  "Join sessions with their recorded usage and group by worker tier (default) or model. Costs are estimates, not the invoice. Sessions without usage rows are listed under `unknown`.",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opts.by != "tier" && opts.by != "model" {
				return usageError{fmt.Errorf("invalid --by %q: use tier or model", opts.by)}
			}
			return ctx.showUsage(cmd.Context(), cmd, opts)
		},
	}
	f := cmd.Flags()
	f.StringVar(&opts.by, "by", "tier", "Group by tier or model")
	f.StringVar(&opts.project, "project", "", "Filter by project ID")
	f.BoolVar(&opts.json, "json", false, "Output as JSON")
	return cmd
}

func (c *commandContext) showUsage(ctx context.Context, cmd *cobra.Command, opts usageOptions) error {
	params := url.Values{}
	if opts.project != "" {
		params.Set("project", opts.project)
	}
	var list sessionListResponse
	if err := c.getJSON(ctx, apiPath("sessions", params), &list); err != nil {
		return err
	}
	usageParams := url.Values{}
	if opts.project != "" {
		usageParams.Set("projectId", opts.project)
	}
	var usage usageCompactResponse
	if err := c.getJSON(ctx, apiPath("usage/sessions", usageParams), &usage); err != nil {
		return err
	}
	rows := groupUsage(list.Sessions, usage.Sessions, opts.by)
	if opts.json {
		return writeJSON(cmd.OutOrStdout(), usageOutput{By: opts.by, Note: usageEstimateNote, Data: rows})
	}
	return writeUsageTable(cmd, rows, opts.by)
}

func groupUsage(sessions []sessionDTO, usage []usageCompactDTO, by string) []usageRow {
	byID := make(map[string]usageCompactDTO, len(usage))
	for _, u := range usage {
		byID[u.SessionID] = u
	}
	groups := map[string]*usageRow{}
	get := func(name string) *usageRow {
		r := groups[name]
		if r == nil {
			r = &usageRow{Group: name}
			groups[name] = r
		}
		return r
	}
	for _, s := range sessions {
		key := s.Tier
		if by == "model" {
			key = s.Model
		}
		if key == "" {
			if by == "tier" {
				key = "standard"
			} else {
				key = usageUnknownGroup
			}
		}
		u, ok := byID[s.ID]
		if !ok {
			// No usage rows: kept apart so a missing reading is never
			// averaged in as a free session.
			get(usageUnknownGroup).Sessions++
			continue
		}
		r := get(key)
		r.Sessions++
		r.SessionsWithUsage++
		if u.ProcessedTokens != nil {
			r.ProcessedTokens += *u.ProcessedTokens
		}
		if u.EstimatedCost != nil {
			r.costNanos += u.EstimatedCost.TotalNanos
			r.costSessions++
		}
	}
	rows := make([]usageRow, 0, len(groups))
	for _, r := range groups {
		r.EstimatedCostUSD = float64(r.costNanos) / 1e9
		if r.costSessions > 0 {
			r.AvgEstimatedCostUSD = r.EstimatedCostUSD / float64(r.costSessions)
		}
		rows = append(rows, *r)
	}
	sort.Slice(rows, func(i, j int) bool {
		iu, ju := rows[i].Group == usageUnknownGroup, rows[j].Group == usageUnknownGroup
		if iu != ju {
			return ju
		}
		return rows[i].Group < rows[j].Group
	})
	return rows
}

func writeUsageTable(cmd *cobra.Command, rows []usageRow, by string) error {
	out := cmd.OutOrStdout()
	if len(rows) == 0 {
		_, err := fmt.Fprintln(out, "(no sessions)")
		return err
	}
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintf(table, "%s\tSESSIONS\tTOKENS (in+out)\tEST. COST\tEST. AVG/SESSION\n", strings.ToUpper(by)); err != nil {
		return err
	}
	for _, r := range rows {
		tokens, cost, avg := "-", "-", "-"
		if r.SessionsWithUsage > 0 {
			tokens = fmt.Sprint(r.ProcessedTokens)
			cost = fmt.Sprintf("$%.4f", r.EstimatedCostUSD)
			avg = fmt.Sprintf("$%.4f", r.AvgEstimatedCostUSD)
		}
		if _, err := fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%s\n", r.Group, r.Sessions, tokens, cost, avg); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, usageEstimateNote)
	return err
}
