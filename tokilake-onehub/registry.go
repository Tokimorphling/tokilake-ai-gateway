package tokilake_onehub

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	tokilake "github.com/Tokimorphling/Tokilake/tokilake-core"
	"one-api/common"
	"one-api/common/config"
	"one-api/model"

	"gorm.io/gorm"
)

type HubWorkerRegistry struct {
	Manager *tokilake.SessionManager
}

func NewHubWorkerRegistry(manager *tokilake.SessionManager) *HubWorkerRegistry {
	return &HubWorkerRegistry{Manager: manager}
}

type controlPlaneError struct {
	code    string
	message string
}

func (e *controlPlaneError) Error() string {
	return e.message
}

func (r *HubWorkerRegistry) RegisterWorker(ctx context.Context, session *tokilake.GatewaySession, register *tokilake.RegisterMessage) (*tokilake.RegisterResult, error) {
	namespace := strings.TrimSpace(register.Namespace)
	if namespace == "" {
		return nil, errors.New("namespace is required")
	}

	if err := r.Manager.ClaimNamespace(session, namespace); err != nil {
		return nil, err
	}

	result, err := r.upsertWorkerAndChannel(session, register)
	if err != nil {
		r.Manager.Release(session)
		return nil, err
	}

	r.Manager.BindChannel(session, result.WorkerID, result.ChannelID, result.Group, result.Models, result.BackendType, result.Status, register.ConcurrencyLimit)
	model.ChannelGroup.Load()
	model.GlobalUserGroupRatio.Load()
	return result, nil
}

func (r *HubWorkerRegistry) UpdateHeartbeat(ctx context.Context, session *tokilake.GatewaySession, heartbeat *tokilake.HeartbeatMessage) error {
	status := r.normalizeWorkerStatus(heartbeat.Status)
	models := session.Models
	modelsChanged := false
	if len(heartbeat.CurrentModels) > 0 {
		models = r.normalizeModels(heartbeat.CurrentModels)
		modelsChanged = !r.stringSlicesEqual(models, session.Models)
	}

	now := time.Now().Unix()
	statusChanged := status != session.Status

	nodeUpdates := map[string]any{
		"status":         status,
		"last_heartbeat": now,
		"updated_at":     now,
	}
	if heartbeat.NodeName != "" {
		nodeUpdates["node_name"] = strings.TrimSpace(heartbeat.NodeName)
	}
	if heartbeat.HardwareInfo != nil {
		if data, err := common.Marshal(heartbeat.HardwareInfo); err == nil {
			nodeUpdates["hardware_info"] = string(data)
		}
	}
	if modelsChanged {
		if data, err := common.Marshal(models); err == nil {
			nodeUpdates["models"] = string(data)
		}
	}

	channelUpdates := map[string]any{
		"status": r.channelStatusFromWorkerStatus(status),
	}
	if modelsChanged {
		channelUpdates["models"] = strings.Join(models, ",")
	}

	if err := model.DB.Model(&model.TokilakeWorkerNode{}).Where("id = ?", session.WorkerID).Updates(nodeUpdates).Error; err != nil {
		return err
	}
	if err := model.DB.Model(&model.Channel{}).Where("id = ?", session.ChannelID).Updates(channelUpdates).Error; err != nil {
		return err
	}

	session.Status = status
	if heartbeat.ConcurrencyLimit > 0 {
		session.ConcurrencyLimit = heartbeat.ConcurrencyLimit
	}
	if modelsChanged {
		session.Models = models
	}

	if statusChanged || modelsChanged {
		model.ChannelGroup.Load()
	}
	return nil
}

func (r *HubWorkerRegistry) SyncModels(ctx context.Context, session *tokilake.GatewaySession, modelsSync *tokilake.ModelsSyncMessage) error {
	models := r.normalizeModels(modelsSync.Models)
	if len(models) == 0 {
		return errors.New("at least one model is required")
	}
	group, err := r.resolveAuthorizedGroup(session.Token.UserId, modelsSync.Group, session.Group)
	if err != nil {
		return err
	}
	backendType := r.normalizeBackendType(modelsSync.BackendType, session.BackendType)

	err = model.DB.Transaction(func(tx *gorm.DB) error {
		channel := &model.Channel{}
		if err := tx.First(channel, "id = ?", session.ChannelID).Error; err != nil {
			return err
		}
		node := &model.TokilakeWorkerNode{}
		if err := tx.First(node, "id = ?", session.WorkerID).Error; err != nil {
			return err
		}
		node.SetModels(models)
		if modelsSync.HardwareInfo != nil {
			node.SetHardwareInfo(modelsSync.HardwareInfo)
		}
		now := time.Now().Unix()
		if err := tx.Model(node).Updates(map[string]any{
			"models":         node.Models,
			"hardware_info":  node.HardwareInfo,
			"last_heartbeat": now,
			"updated_at":     now,
		}).Error; err != nil {
			return err
		}

		if err := r.ensureUserGroups(tx, group); err != nil {
			return err
		}

		channel.Models = strings.Join(models, ",")
		channel.Group = group
		channel.Status = r.channelStatusFromWorkerStatus(session.Status)
		if err := tx.Model(channel).Updates(map[string]any{
			"models":   channel.Models,
			"group":    channel.Group,
			"status":   channel.Status,
			"base_url": r.tokiameChannelBaseURL(session.Namespace),
			"type":     config.ChannelTypeTokiame,
		}).Error; err != nil {
			return err
		}

		session.Models = models
		session.Group = group
		session.BackendType = backendType
		if modelsSync.ConcurrencyLimit > 0 {
			session.ConcurrencyLimit = modelsSync.ConcurrencyLimit
		}
		return nil
	})
	if err == nil {
		model.ChannelGroup.Load()
		model.GlobalUserGroupRatio.Load()
	}
	return err
}

func (r *HubWorkerRegistry) CleanupWorker(ctx context.Context, session *tokilake.GatewaySession) error {
	var err error
	if session.WorkerID != 0 && session.ChannelID != 0 {
		err = model.DB.Transaction(func(tx *gorm.DB) error {
			node := &model.TokilakeWorkerNode{}
			if txErr := tx.First(node, "id = ?", session.WorkerID).Error; txErr != nil {
				if errors.Is(txErr, gorm.ErrRecordNotFound) {
					return nil
				}
				return txErr
			}
			channel := &model.Channel{}
			if txErr := tx.First(channel, "id = ?", session.ChannelID).Error; txErr != nil {
				if errors.Is(txErr, gorm.ErrRecordNotFound) {
					return nil
				}
				return txErr
			}

			now := time.Now().Unix()
			node.Status = model.TokilakeWorkerNodeStatusOffline
			node.LastHeartbeat = now
			node.UpdatedAt = now
			if txErr := tx.Model(node).Updates(map[string]any{
				"status":         node.Status,
				"last_heartbeat": node.LastHeartbeat,
				"updated_at":     node.UpdatedAt,
			}).Error; txErr != nil {
				return txErr
			}

			channel.Status = config.ChannelStatusAutoDisabled
			if txErr := tx.Model(channel).Update("status", channel.Status).Error; txErr != nil {
				return txErr
			}
			return nil
		})
		if err == nil {
			model.ChannelGroup.Load()
		}
	}
	return err
}

func (r *HubWorkerRegistry) upsertWorkerAndChannel(session *tokilake.GatewaySession, register *tokilake.RegisterMessage) (*tokilake.RegisterResult, error) {
	models := r.normalizeModels(register.Models)
	if len(models) == 0 {
		return nil, errors.New("at least one model is required")
	}

	group, err := r.resolveAuthorizedGroup(session.Token.UserId, register.Group, "")
	if err != nil {
		return nil, err
	}
	nodeName := r.normalizeNodeName(register.Namespace, register.NodeName)
	backendType := r.normalizeBackendType(register.BackendType, "")
	status := model.TokilakeWorkerNodeStatusOnline

	result := &tokilake.RegisterResult{
		Namespace:   strings.TrimSpace(register.Namespace),
		Group:       group,
		Models:      models,
		BackendType: backendType,
		Status:      status,
	}

	err = model.DB.Transaction(func(tx *gorm.DB) error {
		now := time.Now().Unix()

		node := &model.TokilakeWorkerNode{}
		err := tx.Where("namespace = ?", result.Namespace).First(node).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			node = &model.TokilakeWorkerNode{}
		} else if node.ProviderId != 0 && node.ProviderId != session.Token.UserId {
			return &controlPlaneError{
				code:    "namespace_not_owned",
				message: fmt.Sprintf("namespace %s is already owned by another user", result.Namespace),
			}
		}

		channel, err := r.loadOrCreateTokiameChannel(tx, node.ChannelId)
		if err != nil {
			return err
		}

		if err := r.ensureUserGroups(tx, group); err != nil {
			return err
		}

		channelName := r.tokiameChannelName(result.Namespace, nodeName)
		baseURL := r.tokiameChannelBaseURL(result.Namespace)
		if channel.Id == 0 {
			channel.Type = config.ChannelTypeTokiame
			channel.Key = ""
			channel.CreatedTime = now
			channel.Weight = &config.DefaultChannelWeight
		}
		channel.Type = config.ChannelTypeTokiame
		channel.Name = channelName
		channel.BaseURL = &baseURL
		channel.Models = strings.Join(models, ",")
		channel.Group = group
		channel.Status = r.channelStatusFromWorkerStatus(status)
		if channel.Id == 0 {
			if err = tx.Create(channel).Error; err != nil {
				return err
			}
		} else {
			if err = tx.Model(channel).Updates(map[string]any{
				"type":     channel.Type,
				"name":     channel.Name,
				"base_url": channel.BaseURL,
				"models":   channel.Models,
				"group":    channel.Group,
				"status":   channel.Status,
			}).Error; err != nil {
				return err
			}
		}

		node.ProviderId = session.Token.UserId
		node.Namespace = result.Namespace
		node.NodeName = nodeName
		node.Status = status
		node.ChannelId = channel.Id
		node.LastHeartbeat = now
		node.UpdatedAt = now
		if node.Id == 0 {
			node.CreatedAt = now
		}
		node.SetModels(models)
		if register.HardwareInfo != nil {
			node.SetHardwareInfo(register.HardwareInfo)
		}

		if node.Id == 0 {
			if err = tx.Create(node).Error; err != nil {
				return err
			}
		} else {
			if err = tx.Model(node).Updates(map[string]any{
				"provider_id":    node.ProviderId,
				"node_name":      node.NodeName,
				"status":         node.Status,
				"models":         node.Models,
				"hardware_info":  node.HardwareInfo,
				"last_heartbeat": node.LastHeartbeat,
				"channel_id":     node.ChannelId,
				"updated_at":     node.UpdatedAt,
			}).Error; err != nil {
				return err
			}
		}

		result.WorkerID = node.Id
		result.ChannelID = channel.Id
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *HubWorkerRegistry) stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (r *HubWorkerRegistry) normalizeModels(models []string) []string {
	seen := make(map[string]struct{}, len(models))
	normalized := make([]string, 0, len(models))
	for _, modelName := range models {
		modelName = strings.TrimSpace(modelName)
		if modelName == "" {
			continue
		}
		if _, ok := seen[modelName]; ok {
			continue
		}
		seen[modelName] = struct{}{}
		normalized = append(normalized, modelName)
	}
	slices.Sort(normalized)
	return normalized
}

func (r *HubWorkerRegistry) normalizeGroup(group string, fallback string) string {
	source := group
	if strings.TrimSpace(source) == "" {
		source = fallback
	}
	if strings.TrimSpace(source) == "" {
		source = "default"
	}
	parts := strings.Split(source, ",")
	seen := make(map[string]struct{}, len(parts))
	groups := make([]string, 0, len(parts))
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		groups = append(groups, item)
	}
	if len(groups) == 0 {
		groups = []string{"default"}
	}
	slices.Sort(groups)
	return strings.Join(groups, ",")
}

func (r *HubWorkerRegistry) resolveAuthorizedGroup(userID int, requested string, fallback string) (string, error) {
	if userID <= 0 {
		return "", errors.New("invalid user id")
	}

	primaryGroup, err := model.CacheGetUserGroup(userID)
	if err != nil {
		return "", err
	}
	primaryGroup = r.normalizeGroup(primaryGroup, "default")

	allowedGroups := map[string]struct{}{
		primaryGroup: {},
	}

	grants, err := model.GetUserPrivateGroupGrantDetails(userID)
	if err != nil {
		return "", err
	}
	for _, grant := range grants {
		groupSlug := strings.TrimSpace(grant.GroupSlug)
		if groupSlug == "" {
			continue
		}
		allowedGroups[groupSlug] = struct{}{}
	}

	source := strings.TrimSpace(requested)
	if source == "" {
		source = strings.TrimSpace(fallback)
	}
	if source == "" {
		return primaryGroup, nil
	}

	group := r.normalizeGroup(source, primaryGroup)
	for _, item := range strings.Split(group, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := allowedGroups[item]; ok {
			continue
		}
		return "", &controlPlaneError{
			code:    "group_not_authorized",
			message: fmt.Sprintf("group %s is not authorized for current user", item),
		}
	}
	return group, nil
}

func (r *HubWorkerRegistry) normalizeNodeName(namespace string, nodeName string) string {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName != "" {
		return nodeName
	}
	return strings.TrimSpace(namespace)
}

func (r *HubWorkerRegistry) normalizeBackendType(backendType string, fallback string) string {
	backendType = strings.TrimSpace(backendType)
	if backendType != "" {
		return backendType
	}
	return strings.TrimSpace(fallback)
}

func (r *HubWorkerRegistry) normalizeWorkerStatus(status int) int {
	switch status {
	case model.TokilakeWorkerNodeStatusBusy:
		return model.TokilakeWorkerNodeStatusBusy
	case model.TokilakeWorkerNodeStatusOffline:
		return model.TokilakeWorkerNodeStatusOffline
	default:
		return model.TokilakeWorkerNodeStatusOnline
	}
}

func (r *HubWorkerRegistry) channelStatusFromWorkerStatus(status int) int {
	if status == model.TokilakeWorkerNodeStatusOffline {
		return config.ChannelStatusAutoDisabled
	}
	return config.ChannelStatusEnabled
}

func (r *HubWorkerRegistry) tokiameChannelName(namespace string, nodeName string) string {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" || nodeName == strings.TrimSpace(namespace) {
		return fmt.Sprintf("Tokiame/%s", strings.TrimSpace(namespace))
	}
	return fmt.Sprintf("Tokiame/%s (%s)", strings.TrimSpace(namespace), nodeName)
}

func (r *HubWorkerRegistry) tokiameChannelBaseURL(namespace string) string {
	return fmt.Sprintf("tokiame://%s", strings.TrimSpace(namespace))
}

func (r *HubWorkerRegistry) ensureUserGroups(tx *gorm.DB, groups string) error {
	for _, group := range strings.Split(strings.TrimSpace(groups), ",") {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		existing := &model.UserGroup{}
		err := tx.Where("symbol = ?", group).First(existing).Error
		if err == nil {
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		enable := true
		userGroup := &model.UserGroup{
			Symbol: group,
			Name:   group,
			Ratio:  1,
			Public: false,
			Enable: &enable,
		}
		if err = tx.Create(userGroup).Error; err != nil {
			if retryErr := tx.Where("symbol = ?", group).First(existing).Error; retryErr == nil {
				continue
			}
			return err
		}
	}
	return nil
}

func (r *HubWorkerRegistry) loadOrCreateTokiameChannel(tx *gorm.DB, channelID int) (*model.Channel, error) {
	channel := &model.Channel{}
	if channelID == 0 {
		return channel, nil
	}
	if err := tx.First(channel, "id = ?", channelID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &model.Channel{}, nil
		}
		return nil, err
	}
	return channel, nil
}
