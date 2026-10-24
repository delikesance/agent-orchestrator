package daemon

import (
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
)

// pipelineAPI hands the service to the HTTP layer without wrapping a nil
// service in a non-nil interface, which would mount live-looking routes over a
// disabled pipeline instead of answering 501.
func pipelineAPI(svc *pipeline.Service) controllers.PipelineService {
	if svc == nil {
		return nil
	}
	return svc
}
