package tokilake

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

const (
	TunnelRouteKindChatCompletions    = "chat_completions"
	TunnelRouteKindCompletions        = "completions"
	TunnelRouteKindResponses          = "responses"
	TunnelRouteKindEmbeddings         = "embeddings"
	TunnelRouteKindRerank             = "rerank"
	TunnelRouteKindAudioSpeech        = "audio_speech"
	TunnelRouteKindAudioTranscription = "audio_transcription"
	TunnelRouteKindAudioTranslation   = "audio_translation"
	TunnelRouteKindImagesGenerations  = "images_generations"
	TunnelRouteKindImagesEdits        = "images_edits"
	TunnelRouteKindImagesVariations   = "images_variations"
	TunnelRouteKindVideosCreate       = "videos_create"
	TunnelRouteKindVideosGet          = "videos_get"
	TunnelRouteKindVideosContent      = "videos_content"
	TunnelRouteKindComfyUIPrompt        = "comfyui_prompt"
	TunnelRouteKindComfyUIWorkflowsList = "comfyui_workflows_list"
	TunnelRouteKindComfyUIWorkflowGet   = "comfyui_workflow_get"
	TunnelRouteKindComfyUIWorkflowRun   = "comfyui_workflow_run"
	TunnelRouteKindComfyUITaskGet       = "comfyui_task_get"
	TunnelRouteKindComfyUIView          = "comfyui_view"
	TunnelRouteKindComfyUIQueueGet      = "comfyui_queue_get"
	TunnelRouteKindComfyUIInterrupt     = "comfyui_interrupt"
)

type TunnelRequest struct {
	RequestID string            `json:"request_id"`
	RouteKind string            `json:"route_kind"`
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Model     string            `json:"model,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	IsStream  bool              `json:"is_stream,omitempty"`
	Body      []byte            `json:"body,omitempty"`
}

type TunnelResponse struct {
	RequestID  string            `json:"request_id"`
	StatusCode int               `json:"status_code,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	BodyChunk  []byte            `json:"body_chunk,omitempty"`
	EOF        bool              `json:"eof,omitempty"`
	Error      *ErrorMessage     `json:"error,omitempty"`
}

type TunnelStreamCodec struct {
	reader  *bufio.Reader
	stream  io.ReadWriter
	writeMu sync.Mutex
}

func NewTunnelStreamCodec(stream io.ReadWriter) *TunnelStreamCodec {
	return &TunnelStreamCodec{
		reader: bufio.NewReader(stream),
		stream: stream,
	}
}

func (c *TunnelStreamCodec) ReadRequest() (*TunnelRequest, error) {
	request := &TunnelRequest{}
	if err := c.readMessage(request); err != nil {
		return nil, err
	}
	return request, nil
}

func (c *TunnelStreamCodec) WriteRequest(request *TunnelRequest) error {
	return c.writeMessage(request)
}

func (c *TunnelStreamCodec) ReadResponse() (*TunnelResponse, error) {
	response := &TunnelResponse{}
	if err := c.readMessage(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *TunnelStreamCodec) WriteResponse(response *TunnelResponse) error {
	return c.writeMessage(response)
}

func (c *TunnelStreamCodec) readMessage(target any) error {
	for {
		line, err := c.reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if err = json.Unmarshal(line, target); err != nil {
			return fmt.Errorf("decode tunnel message: %w", err)
		}
		return nil
	}
}

func (c *TunnelStreamCodec) writeMessage(payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode tunnel message: %w", err)
	}
	data = append(data, '\n')

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err = c.stream.Write(data); err != nil {
		return fmt.Errorf("write tunnel message: %w", err)
	}
	return nil
}
