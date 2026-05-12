package tokilake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"encoding/json"
	"unsafe"

	tokilake "github.com/Tokimorphling/Tokilake/tokilake-core"
)

const tunnelChunkSize = 32 * 1024

var allowedTunnelRequestHeaders = map[string]struct{}{
	"accept":       {},
	"content-type": {},
}

func (c *Client) acceptDataStreams(ctx context.Context, tunnelSession tokilake.TunnelSession, errCh chan<- error) {
	c.info("accept data streams started")
	for {
		stream, err := tunnelSession.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.debug("accept data streams stopped due to context cancellation")
				return
			}
			c.warn("<<< accept data stream failed err=%v", err)
			select {
			case errCh <- fmt.Errorf("accept data stream: %w", err):
			default:
			}
			return
		}

		c.debug("accepted new data stream")
		go c.handleDataStream(ctx, stream)
	}
}

func (c *Client) handleDataStream(ctx context.Context, stream tokilake.TunnelStream) {
	defer stream.Close()

	codec := tokilake.NewTunnelStreamCodec(stream)
	request, err := codec.ReadRequest()
	if err != nil {
		c.warn("<<< read tunnel request failed err=%v", err)
		return
	}

	c.info("<<< received tunnel request request_id=%s route_kind=%s method=%s path=%s model=%s is_stream=%v",
		request.RequestID, request.RouteKind, request.Method, request.Path, request.Model, request.IsStream)

	// ComfyUI workflow routes are handled by the local workflow manager, not proxied.
	if isComfyUIRouteKind(request.RouteKind) {
		c.handleComfyUIRequest(ctx, codec, request)
		return
	}

	target, err := c.resolveModelTarget(request.Model)
	if err != nil {
		c.warn("<<< resolve model target failed request_id=%s model=%s err=%v", request.RequestID, request.Model, err)
		_ = codec.WriteResponse(&tokilake.TunnelResponse{
			RequestID: request.RequestID,
			Error: &tokilake.ErrorMessage{
				Code:    "target_not_found",
				Message: err.Error(),
			},
		})
		return
	}

	c.debug("resolved target request_id=%s model=%s upstream_model=%s url=%s backend_type=%s",
		request.RequestID, target.ModelName, target.UpstreamModel, target.URL, target.BackendType)

	requestURL, err := buildLocalTargetURL(target.URL, request.Path)
	if err != nil {
		c.warn("<<< build local target URL failed request_id=%s url=%s path=%s err=%v",
			request.RequestID, target.URL, request.Path, err)
		_ = codec.WriteResponse(&tokilake.TunnelResponse{
			RequestID: request.RequestID,
			Error: &tokilake.ErrorMessage{
				Code:    "invalid_target_url",
				Message: err.Error(),
			},
		})
		return
	}

	requestHeaders := mergeRequestHeaders(request.Headers, target.Headers)
	requestBody, requestHeaders, err := prepareRequestForTarget(request, requestHeaders, target)
	if err != nil {
		c.warn("<<< prepare request for target failed request_id=%s err=%v", request.RequestID, err)
		_ = codec.WriteResponse(&tokilake.TunnelResponse{
			RequestID: request.RequestID,
			Error: &tokilake.ErrorMessage{
				Code:    "rewrite_request_failed",
				Message: err.Error(),
			},
		})
		return
	}

	c.debug(">>> sending local request request_id=%s url=%s method=%s headers_count=%d body=%s",
		request.RequestID, requestURL, request.Method, len(requestHeaders), byteToStringView(requestBody, 1024))

	response, cleanup, err := c.doLocalRoundTrip(ctx, request.RequestID, request.Method, requestURL, requestBody, requestHeaders)
	if err != nil {
		c.warn("<<< local round trip failed request_id=%s url=%s err=%v", request.RequestID, requestURL, err)
		_ = codec.WriteResponse(&tokilake.TunnelResponse{
			RequestID: request.RequestID,
			Error: &tokilake.ErrorMessage{
				Code:    "local_request_failed",
				Message: err.Error(),
			},
		})
		return
	}
	defer cleanup()

	c.debug(">>> received local response request_id=%s status=%d headers_count=%d",
		request.RequestID, response.StatusCode, len(response.Header))

	response, err = adaptResponseForTarget(request, response, target)
	if err != nil {
		c.warn("<<< adapt local response failed request_id=%s err=%v", request.RequestID, err)
		_ = codec.WriteResponse(&tokilake.TunnelResponse{
			RequestID: request.RequestID,
			Error: &tokilake.ErrorMessage{
				Code:    "adapt_response_failed",
				Message: err.Error(),
			},
		})
		return
	}
	defer response.Body.Close()

	if err = c.writeHTTPResponse(codec, request.RequestID, response); err != nil {
		c.warn("<<< write tunnel response failed request_id=%s err=%v", request.RequestID, err)
	} else {
		c.debug(">>> tunnel response sent successfully request_id=%s", request.RequestID)
	}
}

func (c *Client) doLocalRoundTrip(ctx context.Context, requestID string, method string, requestURL string, body []byte, headers map[string]string) (*http.Response, func(), error) {
	requestCtx, cancel := context.WithCancel(ctx)
	c.trackLocalRequest(requestID, cancel)
	cleanup := func() {
		c.removeLocalRequest(requestID)
		cancel()
	}

	httpRequest, err := http.NewRequestWithContext(requestCtx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		cleanup()
		c.warn("<<< build local request failed request_id=%s url=%s err=%v", requestID, requestURL, err)
		return nil, nil, fmt.Errorf("build request failed: %w", err)
	}
	applyLocalRequestHeaders(httpRequest, headers)

	c.debug(">>> executing local HTTP request request_id=%s %s %s", requestID, method, requestURL)
	response, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		cleanup()
		c.warn("<<< local HTTP request failed request_id=%s url=%s err=%v", requestID, requestURL, err)
		return nil, nil, fmt.Errorf("do request failed: %w", err)
	}
	c.debug("<<< local HTTP response received request_id=%s status=%d", requestID, response.StatusCode)
	return response, cleanup, nil
}

func adaptResponseForTarget(request *tokilake.TunnelRequest, response *http.Response, target *ResolvedTarget) (*http.Response, error) {
	if response == nil || target == nil {
		return response, nil
	}
	adapter := responseAdapterForBackend(target.BackendType)
	return adapter.AdaptResponse(request, response, target)
}

type responseAdapter interface {
	AdaptResponse(request *tokilake.TunnelRequest, response *http.Response, target *ResolvedTarget) (*http.Response, error)
}

type passthroughResponseAdapter struct{}

func (passthroughResponseAdapter) AdaptResponse(_ *tokilake.TunnelRequest, response *http.Response, _ *ResolvedTarget) (*http.Response, error) {
	return response, nil
}

type videoTaskResponseAdapter struct{}

func (videoTaskResponseAdapter) AdaptResponse(request *tokilake.TunnelRequest, response *http.Response, target *ResolvedTarget) (*http.Response, error) {
	if !isVideoTaskJSONResponseRequest(request) || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return response, nil
	}
	contentType := strings.ToLower(response.Header.Get("Content-Type"))
	if contentType != "" && !strings.Contains(contentType, "application/json") {
		return response, nil
	}

	body, err := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("read video response body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		return response, nil
	}

	payload, err := decodeJSONPayload(body)
	if err != nil {
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		return response, nil
	}
	normalizeVideoTaskPayload(payload, target)
	rewrittenBody, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal video response body: %w", err)
	}
	response.Body = io.NopCloser(bytes.NewReader(rewrittenBody))
	response.ContentLength = int64(len(rewrittenBody))
	response.Header.Set("Content-Type", "application/json")
	response.Header.Set("Content-Length", strconv.FormatInt(response.ContentLength, 10))
	return response, nil
}

func responseAdapterForBackend(backendType string) responseAdapter {
	switch normalizeClientBackendType(backendType) {
	case "openai", "sglang", "vllm_omni":
		return videoTaskResponseAdapter{}
	default:
		return passthroughResponseAdapter{}
	}
}

func (c *Client) writeHTTPResponse(codec *tokilake.TunnelStreamCodec, requestID string, response *http.Response) error {
	if response == nil {
		return fmt.Errorf("response is nil")
	}

	// 发送响应头
	tunnelRes := &tokilake.TunnelResponse{
		RequestID:  requestID,
		StatusCode: response.StatusCode,
		Headers:    flattenHTTPHeaders(response.Header),
	}
	if err := codec.WriteResponse(tunnelRes); err != nil {
		c.warn("<<< write response headers failed request_id=%s status=%d err=%v", requestID, response.StatusCode, err)
		return err
	}
	c.debug(">>> response headers sent request_id=%s status=%d", requestID, response.StatusCode)

	buffer := make([]byte, tunnelChunkSize)
	totalBytes := 0
	chunkCount := 0

	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			totalBytes += n
			chunkCount++
			if writeErr := codec.WriteResponse(&tokilake.TunnelResponse{
				RequestID: requestID,
				BodyChunk: append([]byte(nil), buffer[:n]...),
			}); writeErr != nil {
				c.warn("<<< write response body chunk failed request_id=%s chunk=%d bytes=%d err=%v",
					requestID, chunkCount, n, writeErr)
				return writeErr
			}
		}
		if readErr == io.EOF {
			if writeErr := codec.WriteResponse(&tokilake.TunnelResponse{
				RequestID: requestID,
				EOF:       true,
			}); writeErr != nil {
				c.warn("<<< write response EOF failed request_id=%s err=%v", requestID, writeErr)
				return writeErr
			}
			c.debug(">>> response body sent completely request_id=%s total_bytes=%d chunks=%d", requestID, totalBytes, chunkCount)
			return nil
		}
		if readErr != nil {
			_ = codec.WriteResponse(&tokilake.TunnelResponse{
				RequestID: requestID,
				Error: &tokilake.ErrorMessage{
					Code:    "read_local_response_failed",
					Message: readErr.Error(),
				},
			})
			c.warn("<<< read local response body failed request_id=%s total_bytes=%d err=%v", requestID, totalBytes, readErr)
			return readErr
		}
	}
}

func buildLocalTargetURL(baseTarget string, requestPath string) (string, error) {
	baseURL, err := url.Parse(strings.TrimSpace(baseTarget))
	if err != nil {
		return "", fmt.Errorf("parse base target %q: %w", baseTarget, err)
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return "", fmt.Errorf("base target must include scheme and host: %s", baseTarget)
	}

	requestURL, err := url.Parse(strings.TrimSpace(requestPath))
	if err != nil {
		return "", fmt.Errorf("parse request path %q: %w", requestPath, err)
	}

	baseURL.Path = mergeTargetPath(baseURL.Path, requestURL.Path)
	baseURL.RawQuery = requestURL.RawQuery
	return baseURL.String(), nil
}

func mergeTargetPath(basePath string, requestPath string) string {
	baseSegments := splitURLPathSegments(basePath)
	requestSegments := splitURLPathSegments(requestPath)

	switch {
	case len(requestSegments) == 0:
		if len(baseSegments) == 0 {
			return "/"
		}
		return "/" + strings.Join(baseSegments, "/")
	case len(baseSegments) == 0:
		return "/" + strings.Join(requestSegments, "/")
	}

	overlap := 0
	maxOverlap := minInt(len(baseSegments), len(requestSegments))
	for size := maxOverlap; size > 0; size-- {
		if pathSegmentsEqual(baseSegments[len(baseSegments)-size:], requestSegments[:size]) {
			overlap = size
			break
		}
	}

	merged := make([]string, 0, len(baseSegments)+len(requestSegments)-overlap)
	merged = append(merged, baseSegments...)
	merged = append(merged, requestSegments[overlap:]...)
	if len(merged) == 0 {
		return "/"
	}
	return "/" + strings.Join(merged, "/")
}

func splitURLPathSegments(rawPath string) []string {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" || rawPath == "/" {
		return nil
	}
	parts := strings.Split(rawPath, "/")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		segments = append(segments, part)
	}
	if len(segments) == 0 {
		return nil
	}
	return segments
}

func pathSegmentsEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

func applyLocalRequestHeaders(request *http.Request, headers map[string]string) {
	if request == nil {
		return
	}
	for key, value := range headers {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" || strings.EqualFold(key, "Host") {
			continue
		}
		request.Header.Set(key, value)
	}
}

func mergeRequestHeaders(tunnelHeaders map[string]string, targetHeaders map[string]string) map[string]string {

	merged := make(map[string]string, len(tunnelHeaders)+len(targetHeaders))
	for key, value := range tunnelHeaders {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if _, ok := allowedTunnelRequestHeaders[normalizedKey]; !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		merged[key] = value
	}
	for key, value := range targetHeaders {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" || strings.EqualFold(key, "Host") {
			continue
		}
		merged[key] = value
	}
	return merged
}

func prepareRequestForTarget(request *tokilake.TunnelRequest, headers map[string]string, target *ResolvedTarget) ([]byte, map[string]string, error) {
	if headers == nil {
		headers = make(map[string]string)
	}
	if request == nil {
		return nil, headers, nil
	}
	body := request.Body
	if target == nil {
		return body, headers, nil
	}

	if isVideoCreateRequest(request) {
		switch normalizeClientBackendType(target.BackendType) {
		case "vllm_omni":
			return prepareVLLMOmniVideoCreateRequest(body, headers, target)
		case "sglang":
			return prepareSGLangVideoCreateRequest(body, headers, target)
		}
	}

	return rewriteRequestModelForTarget(body, headers, target)
}

func isVideoCreateRequest(request *tokilake.TunnelRequest) bool {
	if request == nil {
		return false
	}
	return request.RouteKind == tokilake.TunnelRouteKindVideosCreate ||
		(request.Method == http.MethodPost && strings.TrimSpace(request.Path) == "/v1/videos")
}

func isVideoTaskJSONResponseRequest(request *tokilake.TunnelRequest) bool {
	if request == nil {
		return false
	}
	if request.RouteKind == tokilake.TunnelRouteKindVideosCreate || request.RouteKind == tokilake.TunnelRouteKindVideosGet {
		return true
	}
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	path := strings.Trim(strings.TrimSpace(request.Path), "/")
	if method == http.MethodPost && path == "v1/videos" {
		return true
	}
	if method == http.MethodGet && strings.HasPrefix(path, "v1/videos/") && !strings.HasSuffix(path, "/content") {
		return true
	}
	return false
}

func rewriteRequestModelForTarget(body []byte, headers map[string]string, target *ResolvedTarget) ([]byte, map[string]string, error) {
	upstreamModel := strings.TrimSpace(target.UpstreamModel)
	if upstreamModel == "" || upstreamModel == strings.TrimSpace(target.ModelName) {
		return body, headers, nil
	}

	contentType := headerValue(headers, "Content-Type")
	switch {
	case strings.HasPrefix(strings.ToLower(contentType), "application/json"):
		rewrittenBody, err := rewriteJSONModelField(body, upstreamModel)
		if err != nil {
			return nil, nil, err
		}
		return rewrittenBody, headers, nil
	case strings.HasPrefix(strings.ToLower(contentType), "application/x-www-form-urlencoded"):
		rewrittenBody, err := rewriteFormModelField(body, upstreamModel)
		if err != nil {
			return nil, nil, err
		}
		return rewrittenBody, headers, nil
	case strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data"):
		rewrittenBody, rewrittenContentType, err := rewriteMultipartModelField(body, contentType, upstreamModel)
		if err != nil {
			return nil, nil, err
		}
		setHeaderValue(headers, "Content-Type", rewrittenContentType)
		return rewrittenBody, headers, nil
	default:
		return body, headers, nil
	}
}

func prepareSGLangVideoCreateRequest(body []byte, headers map[string]string, target *ResolvedTarget) ([]byte, map[string]string, error) {
	contentType := headerValue(headers, "Content-Type")
	if !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return rewriteRequestModelForTarget(body, headers, target)
	}

	payload, err := decodeJSONPayload(body)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare sglang video request: %w", err)
	}
	applyPayloadModel(payload, target)
	if referenceURL, ok := payload["reference_url"]; !ok || isEmptyFieldValue(referenceURL) {
		if imageURL, ok := payload["image_url"]; ok && !isEmptyFieldValue(imageURL) {
			payload["reference_url"] = imageURL
		}
	}
	rewrittenBody, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal sglang video request: %w", err)
	}
	return rewrittenBody, headers, nil
}

func prepareVLLMOmniVideoCreateRequest(body []byte, headers map[string]string, target *ResolvedTarget) ([]byte, map[string]string, error) {
	contentType := strings.ToLower(headerValue(headers, "Content-Type"))
	switch {
	case strings.HasPrefix(contentType, "multipart/form-data"):
		rewrittenBody, rewrittenContentType, err := rewriteMultipartModelField(body, headerValue(headers, "Content-Type"), videoUpstreamModel(target))
		if err != nil {
			return nil, nil, err
		}
		setHeaderValue(headers, "Content-Type", rewrittenContentType)
		return rewrittenBody, headers, nil
	case strings.HasPrefix(contentType, "application/x-www-form-urlencoded"):
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return nil, nil, fmt.Errorf("prepare vllm omni video form request: %w", err)
		}
		values.Set("model", videoUpstreamModel(target))
		rewrittenBody, rewrittenContentType, err := multipartFromValues(values)
		if err != nil {
			return nil, nil, err
		}
		setHeaderValue(headers, "Content-Type", rewrittenContentType)
		return rewrittenBody, headers, nil
	case strings.HasPrefix(contentType, "application/json"), looksLikeJSON(body):
		payload, err := decodeJSONPayload(body)
		if err != nil {
			return nil, nil, fmt.Errorf("prepare vllm omni video json request: %w", err)
		}
		applyPayloadModel(payload, target)
		rewrittenBody, rewrittenContentType, err := multipartFromPayload(payload)
		if err != nil {
			return nil, nil, err
		}
		setHeaderValue(headers, "Content-Type", rewrittenContentType)
		return rewrittenBody, headers, nil
	default:
		return rewriteRequestModelForTarget(body, headers, target)
	}
}

func rewriteJSONModelField(body []byte, upstreamModel string) ([]byte, error) {
	if len(body) == 0 {
		payload := map[string]any{"model": upstreamModel}
		return json.Marshal(payload)
	}
	payload, err := decodeJSONPayload(body)
	if err != nil {
		return nil, fmt.Errorf("rewrite json model: %w", err)
	}
	payload["model"] = upstreamModel
	return json.Marshal(payload)
}

func rewriteFormModelField(body []byte, upstreamModel string) ([]byte, error) {
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, fmt.Errorf("rewrite form model: %w", err)
	}
	values.Set("model", upstreamModel)
	return []byte(values.Encode()), nil
}

func rewriteMultipartModelField(body []byte, contentType string, upstreamModel string) ([]byte, string, error) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, "", fmt.Errorf("parse multipart content type: %w", err)
	}
	boundary := strings.TrimSpace(params["boundary"])
	if boundary == "" {
		return nil, "", fmt.Errorf("multipart boundary is missing")
	}

	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	modelWritten := false

	for {
		part, partErr := reader.NextPart()
		if partErr == io.EOF {
			break
		}
		if partErr != nil {
			return nil, "", fmt.Errorf("read multipart part: %w", partErr)
		}

		mimeHeader := make(textproto.MIMEHeader)
		for key, values := range part.Header {
			for _, value := range values {
				mimeHeader.Add(key, value)
			}
		}
		newPart, createErr := writer.CreatePart(mimeHeader)
		if createErr != nil {
			return nil, "", fmt.Errorf("create multipart part: %w", createErr)
		}
		if part.FormName() == "model" && part.FileName() == "" {
			modelWritten = true
			if _, writeErr := newPart.Write([]byte(upstreamModel)); writeErr != nil {
				_ = part.Close()
				return nil, "", fmt.Errorf("write multipart model field: %w", writeErr)
			}
			if closeErr := part.Close(); closeErr != nil {
				return nil, "", fmt.Errorf("close multipart model field: %w", closeErr)
			}
			continue
		}
		if _, copyErr := io.Copy(newPart, part); copyErr != nil {
			_ = part.Close()
			return nil, "", fmt.Errorf("copy multipart part: %w", copyErr)
		}
		if closeErr := part.Close(); closeErr != nil {
			return nil, "", fmt.Errorf("close multipart part: %w", closeErr)
		}
	}

	if !modelWritten {
		if err = writer.WriteField("model", upstreamModel); err != nil {
			return nil, "", fmt.Errorf("append multipart model field: %w", err)
		}
	}
	if err = writer.Close(); err != nil {
		return nil, "", fmt.Errorf("close multipart writer: %w", err)
	}
	return buffer.Bytes(), writer.FormDataContentType(), nil
}

func decodeJSONPayload(body []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return map[string]any{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	payload := make(map[string]any)
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return payload, nil
}

func applyPayloadModel(payload map[string]any, target *ResolvedTarget) {
	model := videoUpstreamModel(target)
	if model != "" {
		payload["model"] = model
	}
}

func videoUpstreamModel(target *ResolvedTarget) string {
	if target == nil {
		return ""
	}
	return firstNonEmptyString(target.UpstreamModel, target.ModelName)
}

func looksLikeJSON(body []byte) bool {
	return bytes.HasPrefix(bytes.TrimSpace(body), []byte("{"))
}

func multipartFromValues(values url.Values) ([]byte, string, error) {
	payload := make(map[string]any, len(values))
	for key, list := range values {
		if len(list) == 1 {
			payload[key] = list[0]
			continue
		}
		for index, value := range list {
			payload[fmt.Sprintf("%s[%d]", key, index)] = value
		}
	}
	return multipartFromPayload(payload)
}

func multipartFromPayload(payload map[string]any) ([]byte, string, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	keys := make([]string, 0, len(payload))
	for key := range payload {
		if strings.TrimSpace(key) == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, ok, err := multipartFieldString(payload[key])
		if err != nil {
			return nil, "", err
		}
		if !ok {
			continue
		}
		if err = writer.WriteField(key, value); err != nil {
			return nil, "", fmt.Errorf("write multipart field %s: %w", key, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("close multipart writer: %w", err)
	}
	return buffer.Bytes(), writer.FormDataContentType(), nil
}

func multipartFieldString(value any) (string, bool, error) {
	switch typed := value.(type) {
	case nil:
		return "", false, nil
	case string:
		return typed, true, nil
	case json.Number:
		return typed.String(), true, nil
	case bool:
		return strconv.FormatBool(typed), true, nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), true, nil
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), true, nil
	case int:
		return strconv.Itoa(typed), true, nil
	case int64:
		return strconv.FormatInt(typed, 10), true, nil
	case uint64:
		return strconv.FormatUint(typed, 10), true, nil
	default:
		data, err := json.Marshal(typed)
		if err != nil {
			return "", false, fmt.Errorf("marshal multipart field value: %w", err)
		}
		return string(data), true, nil
	}
}

func isEmptyFieldValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(typed) == ""
	default:
		return false
	}
}

func normalizeVideoTaskPayload(payload map[string]any, target *ResolvedTarget) {
	if payload == nil {
		return
	}
	if _, exists := payload["id"]; !exists {
		if taskID, ok := firstPayloadString(payload, "task_id", "video_id"); ok {
			payload["id"] = taskID
		} else if dataMap, ok := payloadMap(payload["data"]); ok {
			if taskID, ok := firstPayloadString(dataMap, "id", "task_id", "video_id"); ok {
				payload["id"] = taskID
			}
		}
	}
	if _, exists := payload["object"]; !exists {
		payload["object"] = "video"
	}
	if _, exists := payload["created"]; !exists {
		if created, ok := firstPayloadNumber(payload, "created_at", "createdAt", "submit_time"); ok {
			payload["created"] = created
		}
	}
	if model, ok := firstPayloadString(payload, "model", "model_name"); ok && strings.TrimSpace(model) != "" {
		payload["model"] = model
	} else if model := strings.TrimSpace(videoUpstreamModel(target)); model != "" {
		payload["model"] = model
	}
	if status, ok := firstPayloadString(payload, "status", "task_status", "state"); ok {
		payload["status"] = normalizeVideoResponseStatus(status)
	}
	if prompt, ok := firstPayloadString(payload, "prompt"); ok {
		payload["prompt"] = prompt
	}
	if size, ok := firstPayloadString(payload, "size"); ok {
		payload["size"] = size
	}
	if url, ok := videoPayloadURL(payload); ok {
		if _, exists := payload["download_url"]; !exists {
			payload["download_url"] = url
		}
		if _, exists := payload["content_url"]; !exists {
			payload["content_url"] = url
		}
	}
	if normalizeVideoResponseStatus(fmt.Sprint(payload["status"])) == "failed" {
		normalizeVideoTaskError(payload)
	}
}

func normalizeVideoResponseStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "submitted":
		return "submitted"
	case "queued", "pending", "queueing":
		return "queued"
	case "processing", "running", "in_progress":
		return "processing"
	case "completed", "success", "succeeded":
		return "completed"
	case "failed", "failure", "error", "cancelled", "canceled":
		return "failed"
	default:
		return strings.TrimSpace(status)
	}
}

func normalizeVideoTaskError(payload map[string]any) {
	if _, exists := payload["error"]; exists {
		return
	}
	if message, ok := firstPayloadString(payload, "error_message", "fail_reason", "failure_reason", "message"); ok && strings.TrimSpace(message) != "" {
		payload["error"] = map[string]any{
			"message": message,
			"type":    "upstream_error",
			"code":    "video_failed",
		}
	}
}

func videoPayloadURL(payload map[string]any) (string, bool) {
	if url, ok := firstPayloadString(payload, "content_url", "download_url", "url", "video_url"); ok && strings.TrimSpace(url) != "" {
		return url, true
	}
	dataList, ok := payloadList(payload["data"])
	if !ok {
		return "", false
	}
	for _, item := range dataList {
		itemMap, ok := payloadMap(item)
		if !ok {
			continue
		}
		if url, ok := firstPayloadString(itemMap, "url", "video_url", "content_url", "download_url"); ok && strings.TrimSpace(url) != "" {
			return url, true
		}
	}
	return "", false
}

func firstPayloadString(payload map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		value, exists := payload[key]
		if !exists {
			continue
		}
		switch typed := value.(type) {
		case string:
			return typed, true
		case json.Number:
			return typed.String(), true
		case float64:
			return strconv.FormatFloat(typed, 'f', -1, 64), true
		case bool:
			return strconv.FormatBool(typed), true
		}
	}
	return "", false
}

func firstPayloadNumber(payload map[string]any, keys ...string) (int64, bool) {
	for _, key := range keys {
		value, exists := payload[key]
		if !exists {
			continue
		}
		switch typed := value.(type) {
		case json.Number:
			if number, err := typed.Int64(); err == nil {
				return number, true
			}
		case float64:
			return int64(typed), true
		case int64:
			return typed, true
		case int:
			return int64(typed), true
		case string:
			number, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
			if err == nil {
				return number, true
			}
		}
	}
	return 0, false
}

func payloadMap(value any) (map[string]any, bool) {
	typed, ok := value.(map[string]any)
	return typed, ok
}

func payloadList(value any) ([]any, bool) {
	typed, ok := value.([]any)
	return typed, ok
}

func flattenHTTPHeaders(headers http.Header) map[string]string {
	flattened := make(map[string]string, len(headers))
	for key, values := range headers {
		if len(values) == 0 {
			continue
		}
		flattened[key] = strings.Join(values, ", ")
	}
	return flattened
}

func headerValue(headers map[string]string, key string) string {
	for headerKey, value := range headers {
		if strings.EqualFold(headerKey, key) {
			return value
		}
	}
	return ""
}

func setHeaderValue(headers map[string]string, key string, value string) {
	for headerKey := range headers {
		if strings.EqualFold(headerKey, key) {
			headers[headerKey] = value
			return
		}
	}
	headers[key] = value
}

func byteToStringView(b []byte, maxLen int) string {
	if len(b) == 0 {
		return ""
	}
	if maxLen >= 0 && len(b) > maxLen {
		return unsafe.String(unsafe.SliceData(b), maxLen) + "...(truncated)"
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
