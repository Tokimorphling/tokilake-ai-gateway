package provider

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	tokilakesvc "github.com/Tokimorphling/Tokilake/tokilake-core"
	"one-api/common/config"
	"one-api/common/logger"
	"one-api/model"
	"one-api/tokilake-onehub/gateway"
	"one-api/types"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/xtaci/smux"
)

func TestProviderVideoMethodsUseTunnelEndpoints(t *testing.T) {
	logger.SetupLogger()

	channelID := 88001
	manager := tokilakesvc.NewSessionManager()
	previousGateway := gateway.Global
	gateway.Global = tokilakesvc.NewGateway(nil, nil, nil, nil, manager)
	t.Cleanup(func() {
		gateway.Global = previousGateway
	})

	requests := make(chan *tokilakesvc.TunnelRequest, 3)
	setupVideoTunnelSession(t, manager, channelID, func(request *tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte) {
		requests <- request

		switch request.Path {
		case "/v1/videos":
			return &tokilakesvc.TunnelResponse{
					RequestID:  request.RequestID,
					StatusCode: http.StatusOK,
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
				}, mustJSON(t, &types.VideoTaskObject{
					ID:      "vid-submit",
					Object:  "video",
					Status:  types.VideoStatusQueued,
					Model:   "video-model",
					Created: 100,
				})
		case "/v1/videos/vid-submit":
			return &tokilakesvc.TunnelResponse{
					RequestID:  request.RequestID,
					StatusCode: http.StatusOK,
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
				}, mustJSON(t, &types.VideoTaskObject{
					ID:      "vid-submit",
					Object:  "video",
					Status:  types.VideoStatusCompleted,
					Model:   "video-model",
					Created: 101,
				})
		case "/v1/videos/vid-submit/content":
			return &tokilakesvc.TunnelResponse{
				RequestID:  request.RequestID,
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"Content-Type":   "video/mp4",
					"Content-Length": "4",
				},
			}, []byte("mp4!")
		default:
			return &tokilakesvc.TunnelResponse{
				RequestID: request.RequestID,
				Error: &tokilakesvc.ErrorMessage{
					Code:    "unexpected_path",
					Message: request.Path,
				},
			}, nil
		}
	})

	channel := &model.Channel{Id: channelID}
	provider := ProviderFactory{}.Create(channel).(*Provider)

	request := &types.VideoRequest{
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
		Prompt: "orbiting camera around a robot",
		ExtraFields: map[string]any{
			"guidance_scale": 7.5,
		},
	}

	submitted, errWithCode := provider.CreateVideo(request)
	require.Nil(t, errWithCode)
	require.Equal(t, "vid-submit", submitted.ID)

	firstReq := <-requests
	require.Equal(t, http.MethodPost, firstReq.Method)
	require.Equal(t, "/v1/videos", firstReq.Path)
	require.Equal(t, "video-model", firstReq.Model)
	require.JSONEq(t, `{
		"model":"video-model",
		"mode":"text2video",
		"prompt":"orbiting camera around a robot",
		"guidance_scale":7.5
	}`, string(firstReq.Body))

	provider.SetOriginalModel("video-model")

	detail, errWithCode := provider.GetVideo("vid-submit")
	require.Nil(t, errWithCode)
	require.Equal(t, types.VideoStatusCompleted, detail.Status)

	secondReq := <-requests
	require.Equal(t, http.MethodGet, secondReq.Method)
	require.Equal(t, "/v1/videos/vid-submit", secondReq.Path)
	require.Equal(t, "video-model", secondReq.Model)

	resp, errWithCode := provider.GetVideoContent("vid-submit")
	require.Nil(t, errWithCode)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("mp4!"), body)
	require.Equal(t, "video/mp4", resp.Header.Get("Content-Type"))

	thirdReq := <-requests
	require.Equal(t, http.MethodGet, thirdReq.Method)
	require.Equal(t, "/v1/videos/vid-submit/content", thirdReq.Path)
	require.Equal(t, "video-model", thirdReq.Model)
}

func TestProviderCreateVideoPreservesMultipartBody(t *testing.T) {
	logger.SetupLogger()
	gin.SetMode(gin.TestMode)

	channelID := 88002
	manager := tokilakesvc.NewSessionManager()
	previousGateway := gateway.Global
	gateway.Global = tokilakesvc.NewGateway(nil, nil, nil, nil, manager)
	t.Cleanup(func() {
		gateway.Global = previousGateway
	})

	requests := make(chan *tokilakesvc.TunnelRequest, 1)
	setupVideoTunnelSession(t, manager, channelID, func(request *tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte) {
		requests <- request
		return &tokilakesvc.TunnelResponse{
				RequestID:  request.RequestID,
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}, mustJSON(t, &types.VideoTaskObject{
				ID:     "vid-multipart",
				Status: types.VideoStatusQueued,
			})
	})

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("model", "video-model"))
	require.NoError(t, writer.WriteField("mode", types.VideoModeImageToVideo))
	require.NoError(t, writer.WriteField("prompt", "animate"))
	part, err := writer.CreateFormFile("input_reference", "input.png")
	require.NoError(t, err)
	_, err = part.Write([]byte("png"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	rawBody := bytes.Clone(body.Bytes())

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", bytes.NewReader(rawBody))
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	c.Set(config.GinRequestBodyKey, rawBody)

	channel := &model.Channel{Id: channelID}
	provider := ProviderFactory{}.Create(channel).(*Provider)
	provider.SetContext(c)

	submitted, errWithCode := provider.CreateVideo(&types.VideoRequest{
		Model:             "video-model",
		Mode:              types.VideoModeImageToVideo,
		Prompt:            "animate",
		HasInputReference: true,
	})
	require.Nil(t, errWithCode)
	require.Equal(t, "vid-multipart", submitted.ID)

	firstReq := <-requests
	require.Equal(t, rawBody, firstReq.Body)
	require.Contains(t, firstReq.Headers["Content-Type"], "multipart/form-data")
}

func mustJSON(t *testing.T, payload any) []byte {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}

func setupVideoTunnelSession(t *testing.T, manager *tokilakesvc.SessionManager, channelID int, responder func(*tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte)) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	clientSession, err := smux.Client(clientConn, smux.DefaultConfig())
	require.NoError(t, err)
	serverSession, err := smux.Server(serverConn, smux.DefaultConfig())
	require.NoError(t, err)

	session := &tokilakesvc.GatewaySession{
		ID:        uint64(channelID),
		Namespace: "video-test-provider",
		ChannelID: channelID,
		Tunnel:    tokilakesvc.NewSMuxTunnelSession(clientSession),
	}
	require.NoError(t, manager.ClaimNamespace(session, session.Namespace))
	manager.BindChannel(session, 1, channelID, "default", []string{"video-model"}, "openai", 1, 0)

	go func() {
		for {
			stream, acceptErr := serverSession.AcceptStream()
			if acceptErr != nil {
				return
			}

			go func() {
				defer stream.Close()

				codec := tokilakesvc.NewTunnelStreamCodec(stream)
				request, readErr := codec.ReadRequest()
				if readErr != nil {
					return
				}

				response, body := responder(request)
				if response == nil {
					return
				}
				require.NoError(t, codec.WriteResponse(response))
				if len(body) > 0 {
					require.NoError(t, codec.WriteResponse(&tokilakesvc.TunnelResponse{
						RequestID: response.RequestID,
						BodyChunk: bytes.Clone(body),
					}))
				}
				require.NoError(t, codec.WriteResponse(&tokilakesvc.TunnelResponse{
					RequestID: response.RequestID,
					EOF:       true,
				}))
			}()
		}
	}()

	t.Cleanup(func() {
		manager.Release(session)
		_ = serverSession.Close()
	})
}
