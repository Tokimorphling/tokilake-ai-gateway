package provider

import (
	"encoding/json"
	"fmt"
	"net/http"

	tokilake "github.com/Tokimorphling/Tokilake/tokilake-core"
	"one-api/types"
)

func (p *Provider) ListComfyUIWorkflows() (*http.Response, *types.OpenAIErrorWithStatusCode) {
	return p.doTunnelHTTPRequest(http.MethodGet, "/comfyui/workflows", tokilake.TunnelRouteKindComfyUIWorkflowsList, p.OriginalModel, nil, "", false)
}

func (p *Provider) GetComfyUIWorkflow(workflowID string) (*http.Response, *types.OpenAIErrorWithStatusCode) {
	return p.doTunnelHTTPRequest(http.MethodGet, "/comfyui/workflows/"+workflowID, tokilake.TunnelRouteKindComfyUIWorkflowGet, p.OriginalModel, nil, "", false)
}

func (p *Provider) RunComfyUIWorkflow(workflowID string, params map[string]any) (*http.Response, *types.OpenAIErrorWithStatusCode) {
	requestBody, err := json.Marshal(params)
	if err != nil {
		return nil, &types.OpenAIErrorWithStatusCode{
			OpenAIError: types.OpenAIError{
				Message: fmt.Sprintf("marshal request failed: %v", err),
				Type:    "comfyui_error",
			},
			StatusCode: http.StatusBadRequest,
		}
	}

	return p.doTunnelHTTPRequest(http.MethodPost, "/comfyui/workflows/"+workflowID+"/run", tokilake.TunnelRouteKindComfyUIWorkflowRun, p.OriginalModel, requestBody, "application/json", false)
}

func (p *Provider) GetComfyUITask(taskID string) (*http.Response, *types.OpenAIErrorWithStatusCode) {
	return p.doTunnelHTTPRequest(http.MethodGet, "/comfyui/tasks/"+taskID, tokilake.TunnelRouteKindComfyUITaskGet, p.OriginalModel, nil, "", false)
}

func (p *Provider) CreateComfyUIPrompt(request map[string]any) (*http.Response, *types.OpenAIErrorWithStatusCode) {
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, &types.OpenAIErrorWithStatusCode{
			OpenAIError: types.OpenAIError{
				Message: fmt.Sprintf("marshal request failed: %v", err),
				Type:    "comfyui_error",
			},
			StatusCode: http.StatusBadRequest,
		}
	}

	return p.doTunnelHTTPRequest(http.MethodPost, "/prompt", tokilake.TunnelRouteKindComfyUIPrompt, p.OriginalModel, requestBody, "application/json", false)
}

func (p *Provider) GetComfyUIView(filename, subfolder, fileType string) (*http.Response, *types.OpenAIErrorWithStatusCode) {
	path := fmt.Sprintf("/view?filename=%s", filename)
	if subfolder != "" {
		path += fmt.Sprintf("&subfolder=%s", subfolder)
	}
	if fileType != "" {
		path += fmt.Sprintf("&type=%s", fileType)
	}

	return p.doTunnelHTTPRequest(http.MethodGet, path, tokilake.TunnelRouteKindComfyUIView, p.OriginalModel, nil, "", false)
}

func (p *Provider) GetComfyUIQueue() (*http.Response, *types.OpenAIErrorWithStatusCode) {
	return p.doTunnelHTTPRequest(http.MethodGet, "/queue", tokilake.TunnelRouteKindComfyUIQueueGet, p.OriginalModel, nil, "", false)
}

func (p *Provider) InterruptComfyUI() (*http.Response, *types.OpenAIErrorWithStatusCode) {
	return p.doTunnelHTTPRequest(http.MethodPost, "/interrupt", tokilake.TunnelRouteKindComfyUIInterrupt, p.OriginalModel, nil, "", false)
}
