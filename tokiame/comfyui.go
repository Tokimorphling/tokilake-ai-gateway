package tokilake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tokilake "github.com/Tokimorphling/Tokilake/tokilake-core"
)

// ComfyUIWorkflowParam describes a single tunable parameter exposed to the caller.
type ComfyUIWorkflowParam struct {
	Key      string `json:"key"`                 // Node path, e.g. "6.inputs.text"
	Name     string `json:"name"`                // Friendly name, e.g. "prompt"
	Type     string `json:"type"`                // "string", "int", "float", "bool"
	Required bool   `json:"required,omitempty"`  // Whether the caller must provide this
	Default  any    `json:"default,omitempty"`    // Default value
	Min      any    `json:"min,omitempty"`        // Minimum value (numeric types)
	Max      any    `json:"max,omitempty"`        // Maximum value (numeric types)
	Desc     string `json:"description,omitempty"`
}

// ComfyUIWorkflowDef is the on-disk definition of a registered workflow.
type ComfyUIWorkflowDef struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Params      []ComfyUIWorkflowParam `json:"params"`
	Template    map[string]any         `json:"-"` // loaded from template file, not serialised
}

// ComfyUI task status constants.
const (
	ComfyUITaskStatusPending   = "pending"
	ComfyUITaskStatusQueued    = "queued"
	ComfyUITaskStatusExecuting = "executing"
	ComfyUITaskStatusCompleted = "completed"
	ComfyUITaskStatusError     = "error"

	comfyuiTaskTTL           = 1 * time.Hour
	comfyuiTaskCleanInterval = 5 * time.Minute
	comfyuiReloadInterval    = 30 * time.Second
)

// ComfyUITask tracks a submitted prompt.
type ComfyUITask struct {
	TaskID    string    `json:"task_id"`
	PromptID  string    `json:"prompt_id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// ComfyUIWorkflowManager manages workflow templates and task state.
type ComfyUIWorkflowManager struct {
	mu        sync.RWMutex
	workflows map[string]*ComfyUIWorkflowDef
	tasks     map[string]*ComfyUITask
	dir       string
	stopCh    chan struct{}
}

// NewComfyUIWorkflowManager creates a new manager, loads workflows, and starts
// background goroutines for task TTL cleanup and workflow hot-reload.
func NewComfyUIWorkflowManager(dir string) *ComfyUIWorkflowManager {
	mgr := &ComfyUIWorkflowManager{
		workflows: make(map[string]*ComfyUIWorkflowDef),
		tasks:     make(map[string]*ComfyUITask),
		dir:       dir,
		stopCh:    make(chan struct{}),
	}
	if dir != "" {
		_ = mgr.LoadWorkflows()
		go mgr.backgroundLoop()
	}
	return mgr
}

// Stop gracefully shuts down background goroutines.
func (m *ComfyUIWorkflowManager) Stop() {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
}

// backgroundLoop runs periodic task cleanup and workflow hot-reload.
func (m *ComfyUIWorkflowManager) backgroundLoop() {
	cleanTicker := time.NewTicker(comfyuiTaskCleanInterval)
	reloadTicker := time.NewTicker(comfyuiReloadInterval)
	defer cleanTicker.Stop()
	defer reloadTicker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-cleanTicker.C:
			m.cleanExpiredTasks()
		case <-reloadTicker.C:
			_ = m.LoadWorkflows()
		}
	}
}

// cleanExpiredTasks removes tasks older than comfyuiTaskTTL.
func (m *ComfyUIWorkflowManager) cleanExpiredTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-comfyuiTaskTTL)
	for id, task := range m.tasks {
		if task.CreatedAt.Before(cutoff) {
			delete(m.tasks, id)
		}
	}
}

// LoadWorkflows scans the workflow directory for *.json schema files.
// For each schema file (e.g. "txt2img.json"), it expects a matching
// template file (e.g. "txt2img.template.json") with the raw ComfyUI API JSON.
func (m *ComfyUIWorkflowManager) LoadWorkflows() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return fmt.Errorf("read workflows dir %s: %w", m.dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Skip template files and non-json
		if strings.HasSuffix(name, ".template.json") || !strings.HasSuffix(name, ".json") {
			continue
		}

		schemaPath := filepath.Join(m.dir, name)
		schemaData, err := os.ReadFile(schemaPath)
		if err != nil {
			continue
		}

		var def ComfyUIWorkflowDef
		if err := json.Unmarshal(schemaData, &def); err != nil {
			continue
		}

		if def.ID == "" {
			def.ID = strings.TrimSuffix(name, ".json")
		}

		// Load the template file
		templatePath := filepath.Join(m.dir, def.ID+".template.json")
		templateData, err := os.ReadFile(templatePath)
		if err != nil {
			continue // No template, skip this workflow
		}

		var template map[string]any
		if err := json.Unmarshal(templateData, &template); err != nil {
			continue
		}
		def.Template = template

		m.workflows[def.ID] = &def
	}

	return nil
}

// ListWorkflows returns all registered workflows (without template data).
func (m *ComfyUIWorkflowManager) ListWorkflows() []ComfyUIWorkflowDef {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]ComfyUIWorkflowDef, 0, len(m.workflows))
	for _, def := range m.workflows {
		result = append(result, ComfyUIWorkflowDef{
			ID:          def.ID,
			Name:        def.Name,
			Description: def.Description,
			Params:      def.Params,
		})
	}
	return result
}

// GetWorkflow returns a single workflow definition.
func (m *ComfyUIWorkflowManager) GetWorkflow(id string) (*ComfyUIWorkflowDef, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	def, ok := m.workflows[id]
	if !ok {
		return nil, false
	}
	// Return a copy without template
	result := &ComfyUIWorkflowDef{
		ID:          def.ID,
		Name:        def.Name,
		Description: def.Description,
		Params:      def.Params,
	}
	return result, true
}

// BuildPrompt takes a workflow ID and caller-provided params, applies them to
// the template, and returns the assembled ComfyUI prompt JSON ready for /prompt.
func (m *ComfyUIWorkflowManager) BuildPrompt(workflowID string, params map[string]any) (map[string]any, error) {
	m.mu.RLock()
	def, ok := m.workflows[workflowID]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("workflow %q not found", workflowID)
	}

	// Deep-copy the template
	templateBytes, err := json.Marshal(def.Template)
	if err != nil {
		return nil, fmt.Errorf("marshal template: %w", err)
	}
	var prompt map[string]any
	if err := json.Unmarshal(templateBytes, &prompt); err != nil {
		return nil, fmt.Errorf("unmarshal template copy: %w", err)
	}

	// Build a name->key mapping from the param definitions
	nameToKey := make(map[string]string, len(def.Params))
	for _, p := range def.Params {
		nameToKey[p.Name] = p.Key
	}

	// Apply caller params
	for name, value := range params {
		key, ok := nameToKey[name]
		if !ok {
			continue // Ignore unknown params
		}
		if err := setNestedValue(prompt, key, value); err != nil {
			return nil, fmt.Errorf("set param %s (key %s): %w", name, key, err)
		}
	}

	// Check required params that weren't provided
	for _, p := range def.Params {
		if p.Required {
			if _, provided := params[p.Name]; !provided {
				if p.Default == nil {
					return nil, fmt.Errorf("required param %q is missing", p.Name)
				}
			}
		}
	}

	return prompt, nil
}

// TrackTask stores a new task.
func (m *ComfyUIWorkflowManager) TrackTask(taskID, promptID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[taskID] = &ComfyUITask{
		TaskID:    taskID,
		PromptID:  promptID,
		Status:    ComfyUITaskStatusPending,
		CreatedAt: time.Now(),
	}
}

// GetTask returns task info.
func (m *ComfyUIWorkflowManager) GetTask(taskID string) (*ComfyUITask, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.tasks[taskID]
	return task, ok
}

// UpdateTaskStatus updates the status of an existing task.
func (m *ComfyUIWorkflowManager) UpdateTaskStatus(taskID, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
		task.Status = status
	}
}

// resolveComfyUITaskStatus parses a ComfyUI history entry to determine the task status.
// ComfyUI history entries have a "status" object with "status_str" and "completed" fields,
// plus "outputs" when finished.
func resolveComfyUITaskStatus(promptHistory any) string {
	historyMap, ok := promptHistory.(map[string]any)
	if !ok {
		return ComfyUITaskStatusCompleted // If we got it in history, assume done
	}

	// Check status object: {"status": {"status_str": "success", "completed": true}}
	if statusObj, ok := historyMap["status"].(map[string]any); ok {
		statusStr, _ := statusObj["status_str"].(string)
		completed, _ := statusObj["completed"].(bool)

		switch {
		case statusStr == "error":
			return ComfyUITaskStatusError
		case completed:
			return ComfyUITaskStatusCompleted
		case statusStr == "success" || statusStr == "":
			// "success" with completed=false can happen during execution
			if _, hasOutputs := historyMap["outputs"]; hasOutputs {
				return ComfyUITaskStatusCompleted
			}
			return ComfyUITaskStatusExecuting
		default:
			return ComfyUITaskStatusExecuting
		}
	}

	// No status object but present in history → completed
	if _, hasOutputs := historyMap["outputs"]; hasOutputs {
		return ComfyUITaskStatusCompleted
	}
	return ComfyUITaskStatusExecuting
}

// setNestedValue sets a value at a dot-separated path in a nested map structure.
// Example: setNestedValue(m, "6.inputs.text", "hello") sets m["6"]["inputs"]["text"] = "hello"
func setNestedValue(m map[string]any, path string, value any) error {
	parts := strings.Split(path, ".")
	current := m

	for i, part := range parts {
		if i == len(parts)-1 {
			// Last part — set the value
			current[part] = value
			return nil
		}

		next, exists := current[part]
		if !exists {
			return fmt.Errorf("path segment %q not found", strings.Join(parts[:i+1], "."))
		}

		switch typed := next.(type) {
		case map[string]any:
			current = typed
		default:
			return fmt.Errorf("path segment %q is not a map (type %T)", strings.Join(parts[:i+1], "."), next)
		}
	}
	return nil
}

// isComfyUIRouteKind returns true if the route kind is a ComfyUI workflow route.
func isComfyUIRouteKind(routeKind string) bool {
	switch routeKind {
	case tokilake.TunnelRouteKindComfyUIWorkflowsList,
		tokilake.TunnelRouteKindComfyUIWorkflowGet,
		tokilake.TunnelRouteKindComfyUIWorkflowRun,
		tokilake.TunnelRouteKindComfyUITaskGet,
		tokilake.TunnelRouteKindComfyUIQueueGet,
		tokilake.TunnelRouteKindComfyUIInterrupt:
		return true
	default:
		return false
	}
}

// handleComfyUIRequest handles ComfyUI workflow route kinds directly,
// without proxying to a local HTTP target.
func (c *Client) handleComfyUIRequest(ctx context.Context, codec *tokilake.TunnelStreamCodec, request *tokilake.TunnelRequest) {
	if c.comfyuiMgr == nil {
		_ = codec.WriteResponse(&tokilake.TunnelResponse{
			RequestID: request.RequestID,
			Error: &tokilake.ErrorMessage{
				Code:    "comfyui_not_configured",
				Message: "no ComfyUI workflows directory configured",
			},
		})
		return
	}

	switch request.RouteKind {
	case tokilake.TunnelRouteKindComfyUIWorkflowsList:
		c.handleComfyUIWorkflowsList(codec, request)
	case tokilake.TunnelRouteKindComfyUIWorkflowGet:
		c.handleComfyUIWorkflowGet(codec, request)
	case tokilake.TunnelRouteKindComfyUIWorkflowRun:
		c.handleComfyUIWorkflowRun(ctx, codec, request)
	case tokilake.TunnelRouteKindComfyUITaskGet:
		c.handleComfyUITaskGet(ctx, codec, request)
	case tokilake.TunnelRouteKindComfyUIQueueGet:
		c.handleComfyUIProxy(ctx, codec, request, http.MethodGet, "/queue")
	case tokilake.TunnelRouteKindComfyUIInterrupt:
		c.handleComfyUIProxy(ctx, codec, request, http.MethodPost, "/interrupt")
	}
}

// handleComfyUIProxy proxies a request directly to a local ComfyUI endpoint (e.g. /queue, /interrupt).
func (c *Client) handleComfyUIProxy(ctx context.Context, codec *tokilake.TunnelStreamCodec, request *tokilake.TunnelRequest, method, path string) {
	target, err := c.resolveModelTarget(request.Model)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusServiceUnavailable, "target_not_found", err.Error())
		return
	}

	targetURL, err := buildLocalTargetURL(target.URL, path)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusInternalServerError, "url_build_failed", err.Error())
		return
	}

	var bodyReader io.Reader
	if len(request.Body) > 0 {
		bodyReader = bytes.NewReader(request.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, targetURL, bodyReader)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusInternalServerError, "request_build_failed", err.Error())
		return
	}
	if len(request.Body) > 0 {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusBadGateway, "comfyui_request_failed", err.Error())
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	respHeaders := map[string]string{
		"Content-Type": resp.Header.Get("Content-Type"),
	}

	_ = codec.WriteResponse(&tokilake.TunnelResponse{
		RequestID:  request.RequestID,
		StatusCode: resp.StatusCode,
		Headers:    respHeaders,
	})
	_ = codec.WriteResponse(&tokilake.TunnelResponse{
		RequestID: request.RequestID,
		BodyChunk: respBody,
	})
	_ = codec.WriteResponse(&tokilake.TunnelResponse{
		RequestID: request.RequestID,
		EOF:       true,
	})
}

func (c *Client) handleComfyUIWorkflowsList(codec *tokilake.TunnelStreamCodec, request *tokilake.TunnelRequest) {
	workflows := c.comfyuiMgr.ListWorkflows()
	body, _ := json.Marshal(map[string]any{
		"workflows": workflows,
	})
	writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
}

func (c *Client) handleComfyUIWorkflowGet(codec *tokilake.TunnelStreamCodec, request *tokilake.TunnelRequest) {
	// Extract workflow ID from path: /comfyui/workflows/{id}
	parts := strings.Split(strings.Trim(request.Path, "/"), "/")
	if len(parts) < 3 {
		writeErrorResponse(codec, request.RequestID, http.StatusBadRequest, "invalid_path", "workflow ID is required")
		return
	}
	workflowID := parts[len(parts)-1]

	def, ok := c.comfyuiMgr.GetWorkflow(workflowID)
	if !ok {
		writeErrorResponse(codec, request.RequestID, http.StatusNotFound, "workflow_not_found", fmt.Sprintf("workflow %q not found", workflowID))
		return
	}

	body, _ := json.Marshal(def)
	writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
}

func (c *Client) handleComfyUIWorkflowRun(ctx context.Context, codec *tokilake.TunnelStreamCodec, request *tokilake.TunnelRequest) {
	// Extract workflow ID from path: /comfyui/workflows/{id}/run
	parts := strings.Split(strings.Trim(request.Path, "/"), "/")
	if len(parts) < 4 {
		writeErrorResponse(codec, request.RequestID, http.StatusBadRequest, "invalid_path", "workflow ID is required")
		return
	}
	workflowID := parts[len(parts)-2] // e.g. ["comfyui", "workflows", "txt2img", "run"]

	// Parse caller params from body
	var reqBody map[string]any
	if len(request.Body) > 0 {
		if err := json.Unmarshal(request.Body, &reqBody); err != nil {
			writeErrorResponse(codec, request.RequestID, http.StatusBadRequest, "invalid_json", err.Error())
			return
		}
	}

	params, _ := reqBody["params"].(map[string]any)
	if params == nil {
		params = reqBody // Allow flat params too
	}

	// Build prompt from template + params
	prompt, err := c.comfyuiMgr.BuildPrompt(workflowID, params)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusBadRequest, "build_prompt_failed", err.Error())
		return
	}

	// Submit to local ComfyUI /prompt
	target, err := c.resolveModelTarget(request.Model)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusServiceUnavailable, "target_not_found", err.Error())
		return
	}

	promptPayload := map[string]any{
		"prompt": prompt,
	}
	promptBody, _ := json.Marshal(promptPayload)

	promptURL, err := buildLocalTargetURL(target.URL, "/prompt")
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusInternalServerError, "url_build_failed", err.Error())
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, promptURL, strings.NewReader(string(promptBody)))
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusInternalServerError, "request_build_failed", err.Error())
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		writeErrorResponse(codec, request.RequestID, http.StatusBadGateway, "comfyui_request_failed", err.Error())
		return
	}
	defer resp.Body.Close()

	// Read ComfyUI response to get prompt_id
	var comfyResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&comfyResp); err != nil {
		writeErrorResponse(codec, request.RequestID, resp.StatusCode, "comfyui_response_invalid", err.Error())
		return
	}

	promptID, _ := comfyResp["prompt_id"].(string)
	taskID := request.RequestID // Use tunnel request ID as task ID

	if promptID != "" {
		c.comfyuiMgr.TrackTask(taskID, promptID)
	}

	result := map[string]any{
		"task_id":   taskID,
		"prompt_id": promptID,
		"status":    "pending",
	}
	body, _ := json.Marshal(result)
	writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
}

func (c *Client) handleComfyUITaskGet(ctx context.Context, codec *tokilake.TunnelStreamCodec, request *tokilake.TunnelRequest) {
	// Extract task ID from path: /comfyui/tasks/{id}
	parts := strings.Split(strings.Trim(request.Path, "/"), "/")
	if len(parts) < 3 {
		writeErrorResponse(codec, request.RequestID, http.StatusBadRequest, "invalid_path", "task ID is required")
		return
	}
	taskID := parts[len(parts)-1]

	task, ok := c.comfyuiMgr.GetTask(taskID)
	if !ok {
		writeErrorResponse(codec, request.RequestID, http.StatusNotFound, "task_not_found", fmt.Sprintf("task %q not found", taskID))
		return
	}

	// Query local ComfyUI /history/{prompt_id} for status
	target, err := c.resolveModelTarget(request.Model)
	if err != nil {
		body, _ := json.Marshal(task)
		writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
		return
	}

	historyURL, err := buildLocalTargetURL(target.URL, "/history/"+task.PromptID)
	if err != nil {
		body, _ := json.Marshal(task)
		writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, historyURL, nil)
	if err != nil {
		body, _ := json.Marshal(task)
		writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
		return
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		body, _ := json.Marshal(task)
		writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
		return
	}
	defer resp.Body.Close()

	var historyResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&historyResp); err != nil {
		body, _ := json.Marshal(task)
		writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
		return
	}

	// Extract status from history
	promptHistory, ok := historyResp[task.PromptID]
	if !ok {
		// Not in history yet — still pending/queued
		body, _ := json.Marshal(task)
		writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
		return
	}

	// Determine precise status from ComfyUI history entry
	status := resolveComfyUITaskStatus(promptHistory)

	// Update tracked status
	c.comfyuiMgr.UpdateTaskStatus(task.TaskID, status)

	// Upload outputs to S3 if configured and task is completed
	var outputURLs []string
	if status == ComfyUITaskStatusCompleted {
		outputURLs = c.uploadComfyUIOutputs(ctx, target, promptHistory)
	}

	result := map[string]any{
		"task_id":   task.TaskID,
		"prompt_id": task.PromptID,
		"status":    status,
		"result":    promptHistory,
	}
	if len(outputURLs) > 0 {
		result["output_urls"] = outputURLs
	}
	body, _ := json.Marshal(result)
	writeJSONResponse(codec, request.RequestID, http.StatusOK, body)
}

// uploadComfyUIOutputs extracts output file references from a ComfyUI history entry,
// downloads each file from local ComfyUI /view, uploads to S3, and returns a list of URLs.
// Returns nil if S3 is not configured or no outputs are found.
func (c *Client) uploadComfyUIOutputs(ctx context.Context, target *ResolvedTarget, promptHistory any) []string {
	if c.s3 == nil || !c.s3.IsConfigured() || target == nil {
		return nil
	}

	historyMap, ok := promptHistory.(map[string]any)
	if !ok {
		return nil
	}

	outputs, ok := historyMap["outputs"].(map[string]any)
	if !ok {
		return nil
	}

	var urls []string

	// Iterate over each node's outputs
	for _, nodeOutput := range outputs {
		nodeMap, ok := nodeOutput.(map[string]any)
		if !ok {
			continue
		}

		// ComfyUI outputs can have "images", "videos", "gifs" etc.
		for _, mediaKey := range []string{"images", "videos", "gifs"} {
			mediaList, ok := nodeMap[mediaKey].([]any)
			if !ok {
				continue
			}

			for _, mediaItem := range mediaList {
				fileInfo, ok := mediaItem.(map[string]any)
				if !ok {
					continue
				}

				filename, _ := fileInfo["filename"].(string)
				subfolder, _ := fileInfo["subfolder"].(string)
				fileType, _ := fileInfo["type"].(string)
				if filename == "" {
					continue
				}

				url, err := c.downloadAndUploadToS3(ctx, target, filename, subfolder, fileType)
				if err != nil {
					c.warn("S3 upload failed for %s: %v", filename, err)
					continue
				}
				urls = append(urls, url)

				// Replace the file reference with the S3 URL in-place
				fileInfo["url"] = url
			}
		}
	}

	return urls
}

// downloadAndUploadToS3 downloads a file from local ComfyUI /view and uploads it to S3.
func (c *Client) downloadAndUploadToS3(ctx context.Context, target *ResolvedTarget, filename, subfolder, fileType string) (string, error) {
	// Build /view URL
	viewPath := fmt.Sprintf("/view?filename=%s", filename)
	if subfolder != "" {
		viewPath += fmt.Sprintf("&subfolder=%s", subfolder)
	}
	if fileType != "" {
		viewPath += fmt.Sprintf("&type=%s", fileType)
	}

	viewURL, err := buildLocalTargetURL(target.URL, viewPath)
	if err != nil {
		return "", fmt.Errorf("build view URL: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, viewURL, nil)
	if err != nil {
		return "", fmt.Errorf("create view request: %w", err)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("download from ComfyUI: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ComfyUI /view returned HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read view response: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = guessContentType(filename)
	}

	// Build S3 key: comfyui/<filename>
	s3Key := "comfyui/" + filename
	if subfolder != "" {
		s3Key = "comfyui/" + subfolder + "/" + filename
	}

	url, err := c.s3.Upload(ctx, data, s3Key, contentType)
	if err != nil {
		return "", fmt.Errorf("S3 upload: %w", err)
	}

	c.info("uploaded ComfyUI output to S3: %s -> %s", filename, url)
	return url, nil
}

// guessContentType guesses the MIME type from a filename extension.
func guessContentType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".webp":
		return "image/webp"
	case ".gif":
		return "image/gif"
	case ".mp4":
		return "video/mp4"
	case ".webm":
		return "video/webm"
	case ".avi":
		return "video/x-msvideo"
	default:
		return "application/octet-stream"
	}
}

// writeJSONResponse writes a complete JSON response through the tunnel.
func writeJSONResponse(codec *tokilake.TunnelStreamCodec, requestID string, statusCode int, body []byte) {
	_ = codec.WriteResponse(&tokilake.TunnelResponse{
		RequestID:  requestID,
		StatusCode: statusCode,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	})
	_ = codec.WriteResponse(&tokilake.TunnelResponse{
		RequestID: requestID,
		BodyChunk: body,
	})
	_ = codec.WriteResponse(&tokilake.TunnelResponse{
		RequestID: requestID,
		EOF:       true,
	})
}

// writeErrorResponse writes an error JSON response through the tunnel.
func writeErrorResponse(codec *tokilake.TunnelStreamCodec, requestID string, statusCode int, code, message string) {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
	writeJSONResponse(codec, requestID, statusCode, body)
}
