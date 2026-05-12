package relay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"one-api/common"
	"one-api/model"
	providersBase "one-api/providers/base"
	"one-api/types"

	"github.com/gin-gonic/gin"
)

func getComfyUIModelAndToken(c *gin.Context) (string, *model.Token, error) {
	modelName := c.Query("model")
	if modelName == "" {
		// try body
		body, err := io.ReadAll(c.Request.Body)
		if err == nil {
			var req struct {
				Model string `json:"model"`
			}
			json.Unmarshal(body, &req)
			modelName = req.Model
			c.Request.Body = io.NopCloser(bytes.NewBuffer(body))
		}
	}
	if modelName == "" {
		return "", nil, fmt.Errorf("model is required")
	}

	tokenId := c.GetInt("token_id")
	token, err := model.GetTokenById(tokenId)
	if err != nil {
		return "", nil, err
	}
	return modelName, token, nil
}

// relayComfyUIResponse is a helper that copies the tunnel response to the client.
func relayComfyUIResponse(c *gin.Context, resp *http.Response, openaiErr *types.OpenAIErrorWithStatusCode) {
	if openaiErr != nil {
		common.AbortWithMessage(c, openaiErr.StatusCode, openaiErr.Message)
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		c.Writer.Header()[k] = v
	}
	c.Writer.WriteHeader(resp.StatusCode)
	io.Copy(c.Writer, resp.Body)
}

// getComfyUIProvider resolves the model and returns a ComfyUIInterface provider.
func getComfyUIProvider(c *gin.Context) (providersBase.ComfyUIInterface, bool) {
	modelName, _, err := getComfyUIModelAndToken(c)
	if err != nil || modelName == "" {
		common.AbortWithMessage(c, http.StatusBadRequest, "Invalid request or missing model")
		return nil, false
	}

	p, _, err := GetProvider(c, modelName)
	if err != nil {
		common.AbortWithMessage(c, http.StatusServiceUnavailable, "No available channel")
		return nil, false
	}

	comfyProvider, ok := p.(providersBase.ComfyUIInterface)
	if !ok {
		common.AbortWithMessage(c, http.StatusServiceUnavailable, "Provider does not support ComfyUI")
		return nil, false
	}

	return comfyProvider, true
}

// RelayComfyUIWorkflowsList lists available workflows on the worker.
func RelayComfyUIWorkflowsList(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}
	resp, openaiErr := provider.ListComfyUIWorkflows()
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUIWorkflowGet returns the parameter schema for a specific workflow.
func RelayComfyUIWorkflowGet(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}
	workflowID := c.Param("id")
	resp, openaiErr := provider.GetComfyUIWorkflow(workflowID)
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUIWorkflowRun submits a workflow execution with parameter overrides.
func RelayComfyUIWorkflowRun(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}
	workflowID := c.Param("id")

	var params map[string]any
	if err := c.ShouldBindJSON(&params); err != nil {
		common.AbortWithMessage(c, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, openaiErr := provider.RunComfyUIWorkflow(workflowID, params)
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUITaskGet queries the status and results of a submitted task.
func RelayComfyUITaskGet(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}
	taskID := c.Param("id")
	resp, openaiErr := provider.GetComfyUITask(taskID)
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUIPrompt allows power users to submit raw ComfyUI API JSON.
func RelayComfyUIPrompt(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}

	var req map[string]any
	if err := c.ShouldBindJSON(&req); err != nil {
		common.AbortWithMessage(c, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	resp, openaiErr := provider.CreateComfyUIPrompt(req)
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUIView retrieves generated images/output files.
func RelayComfyUIView(c *gin.Context) {
	filename := c.Query("filename")
	subfolder := c.Query("subfolder")
	fileType := c.Query("type")

	if filename == "" {
		common.AbortWithMessage(c, http.StatusBadRequest, "filename is required")
		return
	}

	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}

	resp, openaiErr := provider.GetComfyUIView(filename, subfolder, fileType)
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUIQueueGet returns the current ComfyUI queue status.
func RelayComfyUIQueueGet(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}
	resp, openaiErr := provider.GetComfyUIQueue()
	relayComfyUIResponse(c, resp, openaiErr)
}

// RelayComfyUIInterrupt interrupts the currently executing ComfyUI task.
func RelayComfyUIInterrupt(c *gin.Context) {
	provider, ok := getComfyUIProvider(c)
	if !ok {
		return
	}
	resp, openaiErr := provider.InterruptComfyUI()
	relayComfyUIResponse(c, resp, openaiErr)
}
