package task

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	tokilakesvc "github.com/Tokimorphling/Tokilake/tokilake-core"
	"one-api/common"
	"one-api/common/config"
	"one-api/common/logger"
	"one-api/common/objectstore"
	"one-api/model"
	"one-api/providers"
	"one-api/relay/task/base"
	"one-api/tokilake-onehub/gateway"
	hubprovider "one-api/tokilake-onehub/provider"
	"one-api/types"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/xtaci/smux"
)

func TestTokiameVideoTaskInitValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)

	testCases := []struct {
		name string
		body string
		msg  string
	}{
		{
			name: "text2video requires prompt",
			body: `{"model":"video-model","mode":"text2video"}`,
			msg:  "prompt is required for text2video",
		},
		{
			name: "image2video requires exactly one image input",
			body: `{"model":"video-model","mode":"image2video","image_url":"http://a","image_b64_json":"abc"}`,
			msg:  "image2video requires exactly one of image_url, image_b64_json, reference_url, or input_reference",
		},
		{
			name: "image2video requires one image input",
			body: `{"model":"video-model","mode":"image2video"}`,
			msg:  "image2video requires exactly one of image_url, image_b64_json, reference_url, or input_reference",
		},
		{
			name: "n must equal one",
			body: `{"model":"video-model","mode":"text2video","prompt":"ok","n":2}`,
			msg:  "n must be 1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", bytes.NewBufferString(testCase.body))
			c.Request.Header.Set("Content-Type", "application/json")

			task := &TokiameVideoTask{
				TaskBase: base.TaskBase{
					C:        c,
					Platform: model.TaskPlatformTokiameVideo,
				},
			}
			err := task.Init()
			require.NotNil(t, err)
			require.Equal(t, testCase.msg, err.Message)
		})
	}
}

func TestTokiameVideoTaskInitAcceptsMultipartInputReference(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("model", "video-model"))
	require.NoError(t, writer.WriteField("mode", "image2video"))
	require.NoError(t, writer.WriteField("prompt", "animate this frame"))
	require.NoError(t, writer.WriteField("size", "1280x720"))
	part, err := writer.CreateFormFile("input_reference", "input.png")
	require.NoError(t, err)
	_, err = part.Write([]byte("png"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", bytes.NewReader(body.Bytes()))
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())

	task := &TokiameVideoTask{
		TaskBase: base.TaskBase{
			C:        c,
			Platform: model.TaskPlatformTokiameVideo,
		},
	}
	errWithCode := task.Init()
	require.Nil(t, errWithCode)
	require.Equal(t, "video-model", task.Request.Model)
	require.Equal(t, types.VideoModeImageToVideo, task.Request.Mode)
	require.True(t, task.Request.HasInputReference)
	require.Equal(t, "input_reference", propertiesFromRequest(task.Request).ImageSource)
}

func TestPropertiesFromRequestOmitsRawBase64(t *testing.T) {
	properties := propertiesFromRequest(&types.VideoRequest{
		Model:        "video-model",
		Mode:         types.VideoModeImageToVideo,
		Prompt:       "animate this image",
		ImageB64JSON: "aGVsbG8=",
	})

	payload := string(marshalTaskJSON(properties))
	require.Contains(t, payload, `"image_source":"image_b64_json"`)
	require.Contains(t, payload, `"has_image_b64":true`)
	require.NotContains(t, payload, "aGVsbG8=")
}

func TestTokiameVideoTaskSetProviderRejectsNonTokiameChannel(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	createVideoTestChannelWithType(t, config.ChannelTypeOpenAI, "video-group", "video-model")

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", bytes.NewBufferString(`{"model":"video-model","prompt":"orbiting camera"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("id", user.Id)
	c.Set("token_group", user.Group)

	task := &TokiameVideoTask{
		TaskBase: base.TaskBase{
			C:        c,
			Platform: model.TaskPlatformTokiameVideo,
		},
	}
	require.Nil(t, task.Init())

	err := task.SetProvider()
	require.NotNil(t, err)
	require.Equal(t, "provider_not_found", err.Code)
}

func TestListVideosAndGetVideoByIDUseStoredSnapshot(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	task := createVideoTestTask(t, user.Id, model.TaskStatusQueued, &types.VideoTaskObject{
		ID:      "vid-list-1",
		Object:  "video",
		Status:  types.VideoStatusQueued,
		Model:   "video-model",
		Mode:    types.VideoModeTextToVideo,
		Prompt:  "neon rain in tokyo",
		Created: 1234,
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/videos?status=queued&limit=10", nil)
	c.Set("id", user.Id)
	ListVideos(c)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"object":"list"`)
	require.Contains(t, recorder.Body.String(), `"id":"vid-list-1"`)
	require.Contains(t, recorder.Body.String(), `"content_url":"/v1/videos/vid-list-1/content"`)

	detailRecorder := httptest.NewRecorder()
	detailContext, _ := gin.CreateTestContext(detailRecorder)
	detailContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+task.TaskID, nil)
	detailContext.Params = gin.Params{{Key: "id", Value: task.TaskID}}
	detailContext.Set("id", user.Id)
	GetVideoByID(detailContext)
	require.Equal(t, http.StatusOK, detailRecorder.Code)
	require.Contains(t, detailRecorder.Body.String(), `"id":"vid-list-1"`)
	require.Contains(t, detailRecorder.Body.String(), `"status":"queued"`)
}

func TestVideoTaskFromTaskUsesProxyContentURLAndPreservesDirectDownloadURL(t *testing.T) {
	task := &model.Task{
		TaskID:   "vid-direct",
		Status:   model.TaskStatusSuccess,
		Progress: 100,
		Properties: marshalTaskJSON(&types.VideoTaskProperties{
			Model: "video-model",
			Mode:  types.VideoModeTextToVideo,
		}),
		Data: marshalTaskJSON(&types.VideoTaskObject{
			ID:          "vid-direct",
			Status:      types.VideoStatusCompleted,
			ContentURL:  "https://cdn.example.com/video.mp4",
			DownloadURL: "https://cdn.example.com/download.mp4",
		}),
	}

	video := videoTaskFromTask(task)

	require.Equal(t, "/v1/videos/vid-direct/content", video.ContentURL)
	require.Equal(t, "https://cdn.example.com/download.mp4", video.DownloadURL)
}

func TestGetVideoContentReturnsTerminalErrorsAndStreamsSuccess(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	channel := createVideoTestChannel(t, "video-group", "video-model")

	notReadyTask := createVideoTestTask(t, user.Id, model.TaskStatusQueued, &types.VideoTaskObject{
		ID:     "vid-not-ready",
		Status: types.VideoStatusQueued,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
	})
	notReadyTask.ChannelId = channel.Id
	require.NoError(t, notReadyTask.Update())

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+notReadyTask.TaskID+"/content", nil)
	c.Params = gin.Params{{Key: "id", Value: notReadyTask.TaskID}}
	c.Set("id", user.Id)
	GetVideoContent(c)
	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"code":"video_not_ready"`)

	failedTask := createVideoTestTask(t, user.Id, model.TaskStatusFailure, &types.VideoTaskObject{
		ID:     "vid-failed",
		Status: types.VideoStatusFailed,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
		Error: &types.VideoTaskError{
			Message: "upstream failed",
		},
	})
	failedTask.ChannelId = channel.Id
	failedTask.FailReason = "upstream failed"
	require.NoError(t, failedTask.Update())

	failedRecorder := httptest.NewRecorder()
	failedContext, _ := gin.CreateTestContext(failedRecorder)
	failedContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+failedTask.TaskID+"/content", nil)
	failedContext.Params = gin.Params{{Key: "id", Value: failedTask.TaskID}}
	failedContext.Set("id", user.Id)
	GetVideoContent(failedContext)
	require.Equal(t, http.StatusBadGateway, failedRecorder.Code)
	require.Contains(t, failedRecorder.Body.String(), `"code":"video_failed"`)

	successTask := createVideoTestTask(t, user.Id, model.TaskStatusSuccess, &types.VideoTaskObject{
		ID:     "vid-success",
		Status: types.VideoStatusCompleted,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
	})
	successTask.ChannelId = channel.Id
	successTask.Properties = marshalTaskJSON(&types.VideoTaskProperties{
		Model: "video-model",
		Mode:  types.VideoModeTextToVideo,
	})
	require.NoError(t, successTask.Update())

	setupVideoTaskTunnelSession(t, gateway.Global.Manager, channel.Id, func(request *tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "/v1/videos/vid-success/content", request.Path)
		require.Equal(t, "video-model", request.Model)
		return &tokilakesvc.TunnelResponse{
			RequestID:  request.RequestID,
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type":   "video/mp4",
				"Content-Length": "4",
			},
		}, []byte("mp4!")
	})

	successRecorder := httptest.NewRecorder()
	successContext, _ := gin.CreateTestContext(successRecorder)
	successContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+successTask.TaskID+"/content", nil)
	successContext.Params = gin.Params{{Key: "id", Value: successTask.TaskID}}
	successContext.Set("id", user.Id)
	GetVideoContent(successContext)
	require.Equal(t, http.StatusOK, successRecorder.Code)
	require.Equal(t, "video/mp4", successRecorder.Header().Get("Content-Type"))
	require.Equal(t, "mp4!", successRecorder.Body.String())
}

func TestGetVideoContentRedirectsStoredDirectDownloadURL(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	task := createVideoTestTask(t, user.Id, model.TaskStatusSuccess, &types.VideoTaskObject{
		ID:          "vid-direct-download",
		Status:      types.VideoStatusCompleted,
		Model:       "video-model",
		Mode:        types.VideoModeTextToVideo,
		DownloadURL: "https://cdn.example.com/video.mp4",
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+task.TaskID+"/content", nil)
	c.Params = gin.Params{{Key: "id", Value: task.TaskID}}
	c.Set("id", user.Id)

	GetVideoContent(c)

	require.Equal(t, http.StatusTemporaryRedirect, recorder.Code)
	require.Equal(t, "https://cdn.example.com/video.mp4", recorder.Header().Get("Location"))
}

func TestGetVideoContentRedirectsStoredObjectStorageURL(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	task := createVideoTestTask(t, user.Id, model.TaskStatusSuccess, &types.VideoTaskObject{
		ID:     "vid-object-storage",
		Status: types.VideoStatusCompleted,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
		Storage: &types.VideoStorage{
			Provider: "S3",
			Bucket:   "video-bucket",
			Key:      "videos/vid-object-storage.mp4",
			URL:      "https://storage.example.com/videos/vid-object-storage.mp4",
		},
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+task.TaskID+"/content", nil)
	c.Params = gin.Params{{Key: "id", Value: task.TaskID}}
	c.Set("id", user.Id)

	GetVideoContent(c)

	require.Equal(t, http.StatusTemporaryRedirect, recorder.Code)
	require.Equal(t, "https://storage.example.com/videos/vid-object-storage.mp4", recorder.Header().Get("Location"))
}

func TestUpdateTokiameVideoTasksRefreshesStatusFromWorker(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	channel := createVideoTestChannel(t, "video-group", "video-model")
	task := createVideoTestTask(t, user.Id, model.TaskStatusQueued, &types.VideoTaskObject{
		ID:     "vid-refresh",
		Status: types.VideoStatusQueued,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
	})
	task.ChannelId = channel.Id
	task.Properties = marshalTaskJSON(&types.VideoTaskProperties{
		Model: "video-model",
		Mode:  types.VideoModeTextToVideo,
	})
	require.NoError(t, task.Update())
	model.ChannelGroup.Load()

	setupVideoTaskTunnelSession(t, gateway.Global.Manager, channel.Id, func(request *tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "/v1/videos/vid-refresh", request.Path)
		require.Equal(t, "video-model", request.Model)
		return &tokilakesvc.TunnelResponse{
				RequestID:  request.RequestID,
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			}, mustVideoJSON(t, &types.VideoTaskObject{
				ID:      "vid-refresh",
				Object:  "video",
				Status:  types.VideoStatusCompleted,
				Model:   "video-model",
				Mode:    types.VideoModeTextToVideo,
				Created: 200,
			})
	})

	taskMap := map[string]*model.Task{task.TaskID: task}
	err := updateTokiameVideoTasks(context.Background(), channel.Id, []string{task.TaskID}, taskMap)
	require.NoError(t, err)

	refreshed, err := model.GetTaskByTaskId(model.TaskPlatformTokiameVideo, user.Id, task.TaskID)
	require.NoError(t, err)
	require.Equal(t, model.TaskStatus(model.TaskStatusSuccess), refreshed.Status)
	require.Equal(t, 100, refreshed.Progress)
	require.Contains(t, string(refreshed.Data), `"status":"completed"`)
}

func TestUpdateTokiameVideoTasksUploadsCompletedVideoToObjectStorage(t *testing.T) {
	setupTokiameVideoTestDB(t)
	fakeStorage := &fakeVideoObjectStore{provider: "FakeVideoObjectStore"}
	restoreStore := objectstore.SetStoreForTest(fakeStorage)
	t.Cleanup(restoreStore)
	viper.Set("storage.video.enabled", true)
	viper.Set("storage.video.prefix", "video-results")

	user := createVideoTestUser(t, "video-group")
	channel := createVideoTestChannel(t, "video-group", "video-model")
	task := createVideoTestTask(t, user.Id, model.TaskStatusQueued, &types.VideoTaskObject{
		ID:     "vid-storage",
		Status: types.VideoStatusQueued,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
	})
	task.ChannelId = channel.Id
	task.Properties = marshalTaskJSON(&types.VideoTaskProperties{
		Model: "video-model",
		Mode:  types.VideoModeTextToVideo,
	})
	require.NoError(t, task.Update())
	model.ChannelGroup.Load()

	setupVideoTaskTunnelSession(t, gateway.Global.Manager, channel.Id, func(request *tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte) {
		require.Equal(t, http.MethodGet, request.Method)
		switch request.Path {
		case "/v1/videos/vid-storage":
			return &tokilakesvc.TunnelResponse{
					RequestID:  request.RequestID,
					StatusCode: http.StatusOK,
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
				}, mustVideoJSON(t, &types.VideoTaskObject{
					ID:      "vid-storage",
					Object:  "video",
					Status:  types.VideoStatusCompleted,
					Model:   "video-model",
					Mode:    types.VideoModeTextToVideo,
					Created: 200,
				})
		case "/v1/videos/vid-storage/content":
			return &tokilakesvc.TunnelResponse{
				RequestID:  request.RequestID,
				StatusCode: http.StatusOK,
				Headers: map[string]string{
					"Content-Type":   "video/mp4",
					"Content-Length": "4",
				},
			}, []byte("mp4!")
		default:
			t.Fatalf("unexpected request path: %s", request.Path)
			return nil, nil
		}
	})

	taskMap := map[string]*model.Task{task.TaskID: task}
	err := updateTokiameVideoTasks(context.Background(), channel.Id, []string{task.TaskID}, taskMap)
	require.NoError(t, err)

	refreshed, err := model.GetTaskByTaskId(model.TaskPlatformTokiameVideo, user.Id, task.TaskID)
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusSuccess, refreshed.Status)
	require.Equal(t, []byte("mp4!"), fakeStorage.uploadedData)
	require.Equal(t, "video-results/vid-storage.mp4", fakeStorage.uploadedKey)

	video := videoTaskFromTask(refreshed)
	require.Equal(t, "/v1/videos/vid-storage/content", video.ContentURL)
	require.Equal(t, "https://storage.example.com/video-results/vid-storage.mp4", video.DownloadURL)
	require.NotNil(t, video.Storage)
	require.Equal(t, "FakeVideoObjectStore", video.Storage.Provider)
	require.Equal(t, "video-bucket", video.Storage.Bucket)
	require.Equal(t, "video-results/vid-storage.mp4", video.Storage.Key)
	require.Equal(t, "https://storage.example.com/video-results/vid-storage.mp4", video.Storage.URL)
}

func TestReadVideoContentForStorageStreamsAndLimits(t *testing.T) {
	viper.Set("storage.video.max_size_mb", 1)
	t.Cleanup(func() {
		viper.Set("storage.video.max_size_mb", nil)
	})

	resp := &http.Response{
		Body:          io.NopCloser(bytes.NewReader([]byte("webm!"))),
		ContentLength: -1,
		Header:        http.Header{"Content-Type": []string{"video/webm"}},
	}
	reader, contentType, extension, err := readVideoContentForStorage(resp)
	require.NoError(t, err)
	require.Equal(t, "video/webm", contentType)
	require.Equal(t, ".webm", extension)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("webm!"), data)

	oversized := bytes.Repeat([]byte("x"), 1024*1024+1)
	resp = &http.Response{
		Body:          io.NopCloser(bytes.NewReader(oversized)),
		ContentLength: -1,
		Header:        http.Header{"Content-Type": []string{"video/mp4"}},
	}
	reader, _, _, err = readVideoContentForStorage(resp)
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	require.ErrorContains(t, err, "video content is too large")
}

func TestUpdateTokiameVideoTasksFailsAfterRepeatedPollErrors(t *testing.T) {
	setupTokiameVideoTestDB(t)

	user := createVideoTestUser(t, "video-group")
	channel := createVideoTestChannel(t, "video-group", "video-model")
	task := createVideoTestTask(t, user.Id, model.TaskStatusQueued, &types.VideoTaskObject{
		ID:     "vid-poll-error",
		Status: types.VideoStatusQueued,
		Model:  "video-model",
		Mode:   types.VideoModeTextToVideo,
	})
	task.ChannelId = channel.Id
	task.Properties = marshalTaskJSON(&types.VideoTaskProperties{
		Model:          "video-model",
		Mode:           types.VideoModeTextToVideo,
		PollErrorCount: videoPollErrorFailureThreshold - 1,
	})
	require.NoError(t, task.Update())
	model.ChannelGroup.Load()

	setupVideoTaskTunnelSession(t, gateway.Global.Manager, channel.Id, func(request *tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "/v1/videos/vid-poll-error", request.Path)
		return &tokilakesvc.TunnelResponse{
			RequestID:  request.RequestID,
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		}, []byte(`{"error":{"message":"backend lost task","code":"not_found"}}`)
	})

	taskMap := map[string]*model.Task{task.TaskID: task}
	err := updateTokiameVideoTasks(context.Background(), channel.Id, []string{task.TaskID}, taskMap)
	require.NoError(t, err)

	refreshed, err := model.GetTaskByTaskId(model.TaskPlatformTokiameVideo, user.Id, task.TaskID)
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusFailure, refreshed.Status)
	require.Equal(t, 100, refreshed.Progress)
	require.Contains(t, refreshed.FailReason, "视频任务轮询连续失败")
	require.Contains(t, refreshed.FailReason, "backend lost task")
	require.Contains(t, string(refreshed.Data), `"status":"failed"`)
	require.Contains(t, string(refreshed.Data), `"video_poll_failed"`)
}

func setupTokiameVideoTestDB(t *testing.T) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	viper.Reset()
	config.InitConf()
	viper.Set("sqlite_path", filepath.Join(t.TempDir(), "tokiame-video-test.db"))

	common.UsingPostgreSQL = false
	common.UsingSQLite = false
	config.IsMasterNode = true
	logger.SetupLogger()

	err := model.InitDB()
	require.NoError(t, err)

	previousGateway := gateway.Global
	gateway.Global = tokilakesvc.NewGateway(nil, nil, nil, nil, tokilakesvc.NewSessionManager())
	providers.RegisterProvider(config.ChannelTypeTokiame, hubprovider.ProviderFactory{})

	sqlDB, err := model.DB.DB()
	require.NoError(t, err)

	model.ChannelGroup.Load()
	model.GlobalUserGroupRatio.Load()

	t.Cleanup(func() {
		_ = sqlDB.Close()
		gateway.Global = previousGateway
		viper.Reset()
		common.UsingPostgreSQL = false
		common.UsingSQLite = false
	})
}

func createVideoTestUser(t *testing.T, group string) *model.User {
	t.Helper()

	user := &model.User{
		Username:    "video-user-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Password:    "password123",
		DisplayName: "Video User",
		Role:        config.RoleCommonUser,
		Status:      config.UserStatusEnabled,
		AccessToken: "access-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Group:       group,
		AffCode:     "aff-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		CreatedTime: time.Now().Unix(),
	}
	require.NoError(t, model.DB.Create(user).Error)
	return user
}

func createVideoTestChannel(t *testing.T, group string, modelName string) *model.Channel {
	return createVideoTestChannelWithType(t, config.ChannelTypeTokiame, group, modelName)
}

func createVideoTestChannelWithType(t *testing.T, channelType int, group string, modelName string) *model.Channel {
	t.Helper()

	weight := uint(1)
	priority := int64(0)
	baseURL := "tokiame://video-test"
	channel := &model.Channel{
		Type:        channelType,
		Status:      config.ChannelStatusEnabled,
		Name:        "Tokiame Video",
		Weight:      &weight,
		Priority:    &priority,
		CreatedTime: time.Now().Unix(),
		BaseURL:     &baseURL,
		Models:      modelName,
		Group:       group,
	}
	require.NoError(t, model.DB.Create(channel).Error)
	model.ChannelGroup.Load()
	return channel
}

func createVideoTestTask(t *testing.T, userID int, status model.TaskStatus, payload *types.VideoTaskObject) *model.Task {
	t.Helper()

	task := &model.Task{
		TaskID:     payload.ID,
		Platform:   model.TaskPlatformTokiameVideo,
		UserId:     userID,
		Status:     status,
		Action:     taskActionFromMode(payload.Mode),
		SubmitTime: time.Now().Unix(),
		Progress:   videoStatusToProgress(payload.Status),
		Properties: marshalTaskJSON(&types.VideoTaskProperties{
			Model:  payload.Model,
			Mode:   payload.Mode,
			Prompt: payload.Prompt,
			Size:   payload.Size,
		}),
		Data: marshalTaskJSON(payload),
	}
	require.NoError(t, task.Insert())
	return task
}

func mustVideoJSON(t *testing.T, payload any) []byte {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}

func setupVideoTaskTunnelSession(t *testing.T, manager *tokilakesvc.SessionManager, channelID int, responder func(*tokilakesvc.TunnelRequest) (*tokilakesvc.TunnelResponse, []byte)) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	clientSession, err := smux.Client(clientConn, smux.DefaultConfig())
	require.NoError(t, err)
	serverSession, err := smux.Server(serverConn, smux.DefaultConfig())
	require.NoError(t, err)

	session := &tokilakesvc.GatewaySession{
		ID:        uint64(channelID),
		Namespace: "video-test-" + strconv.Itoa(channelID),
		ChannelID: channelID,
		Tunnel:    tokilakesvc.NewSMuxTunnelSession(clientSession),
	}
	require.NoError(t, manager.ClaimNamespace(session, session.Namespace))
	manager.BindChannel(session, 1, channelID, "video-group", []string{"video-model"}, "openai", 1, 0)

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

type fakeVideoObjectStore struct {
	provider     string
	uploadedKey  string
	uploadedData []byte
}

func (f *fakeVideoObjectStore) Provider() string {
	return f.provider
}

func (f *fakeVideoObjectStore) BucketName() string {
	return "video-bucket"
}

func (f *fakeVideoObjectStore) PutObject(_ context.Context, key string, body io.Reader, _ string) (*objectstore.Object, error) {
	f.uploadedKey = key
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	f.uploadedData = bytes.Clone(data)
	url, err := f.GetObjectURL(context.Background(), key, 0)
	if err != nil {
		return nil, err
	}
	return &objectstore.Object{
		Provider: f.Provider(),
		Bucket:   f.BucketName(),
		Key:      key,
		URL:      url,
	}, nil
}

func (f *fakeVideoObjectStore) GetObjectURL(_ context.Context, key string, _ time.Duration) (string, error) {
	return "https://storage.example.com/" + key, nil
}

func (f *fakeVideoObjectStore) PresignPutObject(_ context.Context, key string, _ string, _ time.Duration) (*objectstore.PresignedRequest, error) {
	return &objectstore.PresignedRequest{
		Provider: f.Provider(),
		Bucket:   f.BucketName(),
		Key:      key,
		Method:   http.MethodPut,
		URL:      "https://storage.example.com/" + key,
	}, nil
}
