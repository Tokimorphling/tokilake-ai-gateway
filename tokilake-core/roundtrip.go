package tokilake

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type TunnelStreamError struct {
	StatusCode int
	Code       string
	Message    string
	Type       string
}

func (e *TunnelStreamError) Error() string {
	if e == nil {
		return "tokiame stream error"
	}
	return e.Message
}

func (g *Gateway) DoTunnelRequest(ctx context.Context, channelID int, request *TunnelRequest) (*http.Response, string, error) {
	session, ok := g.Manager.GetSessionByChannelID(channelID)
	if !ok || session == nil || session.Tunnel == nil {
		return nil, "", fmt.Errorf("tokiame session is offline for channel %d", channelID)
	}
	return g.doTunnelRequestWithSession(ctx, session, channelID, request)
}

func (g *Gateway) DoTunnelRequestByNamespace(ctx context.Context, namespace string, request *TunnelRequest) (*http.Response, string, error) {
	session, ok := g.Manager.GetSessionByNamespace(strings.TrimSpace(namespace))
	if !ok || session == nil || session.Tunnel == nil {
		return nil, "", fmt.Errorf("tokiame session is offline for namespace %s", namespace)
	}
	return g.doTunnelRequestWithSession(ctx, session, session.ChannelID, request)
}

func (g *Gateway) doTunnelRequestWithSession(ctx context.Context, session *GatewaySession, channelID int, request *TunnelRequest) (*http.Response, string, error) {
	if request == nil {
		return nil, "", fmt.Errorf("tunnel request is nil")
	}

	if !session.TryAcquireRequest() {
		return nil, "", fmt.Errorf("concurrency limit exceeded")
	}

	stream, err := session.Tunnel.OpenStream(ctx)
	if err != nil {
		session.ReleaseRequest()
		return nil, "", fmt.Errorf("open tokiame stream: %w", err)
	}

	requestID := strings.TrimSpace(request.RequestID)
	if requestID == "" {
		requestID = buildTunnelRequestID(session.Namespace)
		request.RequestID = requestID
	}

	requestCtx, cancel := context.WithCancel(ctx)
	g.Manager.TrackRequest(&InFlightRequest{
		RequestID: requestID,
		SessionID: session.ID,
		Namespace: session.Namespace,
		ChannelID: channelID,
		CreatedAt: time.Now(),
		Cancel:    cancel,
	})

	codec := NewTunnelStreamCodec(stream)
	if err = codec.WriteRequest(request); err != nil {
		_ = stream.Close()
		g.Manager.RemoveRequest(requestID)
		cancel()
		return nil, requestID, err
	}

	completed := make(chan struct{})
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			close(completed)
			cancel()
			g.Manager.RemoveRequest(requestID)
			session.ReleaseRequest()
		})
	}

	go g.watchTunnelContext(requestCtx, session, stream, requestID, completed)

	firstFrame, err := codec.ReadResponse()
	if err != nil {
		_ = stream.Close()
		cleanup()
		return nil, requestID, fmt.Errorf("read tokiame response header: %w", err)
	}
	if firstFrame.Error != nil {
		_ = stream.Close()
		cleanup()
		return nil, requestID, fmt.Errorf("tokiame request failed: %s", firstFrame.Error.Message)
	}
	if firstFrame.StatusCode == 0 {
		firstFrame.StatusCode = http.StatusBadGateway
	}

	pipeReader, pipeWriter := io.Pipe()
	response := &http.Response{
		StatusCode:    firstFrame.StatusCode,
		Status:        fmt.Sprintf("%d %s", firstFrame.StatusCode, http.StatusText(firstFrame.StatusCode)),
		Header:        expandHeaders(firstFrame.Headers),
		Body:          pipeReader,
		ContentLength: -1,
	}
	go pumpTunnelBody(stream, codec, pipeWriter, firstFrame, cleanup)

	return response, requestID, nil
}

func (g *Gateway) watchTunnelContext(ctx context.Context, session *GatewaySession, stream io.Closer, requestID string, completed chan struct{}) {
	select {
	case <-completed:
		return
	case <-ctx.Done():
		select {
		case <-completed:
			return
		default:
		}
		_ = g.SendCancelRequest(session, requestID, "client_disconnected")
		if stream != nil {
			_ = stream.Close()
		}
	}
}

func pumpTunnelBody(stream io.ReadWriteCloser, codec *TunnelStreamCodec, pipeWriter *io.PipeWriter, firstFrame *TunnelResponse, cleanup func()) {
	defer stream.Close()
	defer cleanup()

	if firstFrame != nil && len(firstFrame.BodyChunk) > 0 {
		if _, err := pipeWriter.Write(firstFrame.BodyChunk); err != nil {
			_ = pipeWriter.CloseWithError(err)
			return
		}
	}
	if firstFrame != nil && firstFrame.EOF {
		_ = pipeWriter.Close()
		return
	}

	for {
		frame, err := codec.ReadResponse()
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
			return
		}
		if frame.Error != nil {
			_ = pipeWriter.CloseWithError(newTunnelStreamError(frame.Error))
			return
		}
		if len(frame.BodyChunk) > 0 {
			if _, err = pipeWriter.Write(frame.BodyChunk); err != nil {
				_ = pipeWriter.CloseWithError(err)
				return
			}
		}
		if frame.EOF {
			_ = pipeWriter.Close()
			return
		}
	}
}

func newTunnelStreamError(errMsg *ErrorMessage) error {
	if errMsg == nil {
		return &TunnelStreamError{
			StatusCode: http.StatusBadGateway,
			Message:    "tokiame stream error",
			Type:       "upstream_error",
			Code:       "tokiame_stream_error",
		}
	}

	code := strings.TrimSpace(errMsg.Code)
	if code == "" {
		code = "tokiame_stream_error"
	}
	message := strings.TrimSpace(errMsg.Message)
	if message == "" {
		message = "tokiame stream error"
	}

	return &TunnelStreamError{
		StatusCode: http.StatusBadGateway,
		Message:    message,
		Type:       "upstream_error",
		Code:       code,
	}
}

func buildTunnelRequestID(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = "tokiame"
	}
	return fmt.Sprintf("%s:relay:%s", namespace, uuid.NewString())
}

func expandHeaders(headers map[string]string) http.Header {
	result := make(http.Header)
	for key, value := range headers {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		result.Set(key, value)
	}
	return result
}
