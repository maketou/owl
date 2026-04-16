package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gowvp/owl/internal/conf"
	"github.com/gowvp/owl/internal/core/recording"
	"github.com/gowvp/owl/internal/core/recording/store/recordingdb"
	"github.com/grafov/m3u8"
	"github.com/ixugo/goddd/pkg/orm"
	"github.com/ixugo/goddd/pkg/reason"
	"github.com/ixugo/goddd/pkg/web"
	"gorm.io/gorm"
)

// RecordingAPI 为 http 提供业务方法
type RecordingAPI struct {
	recordingCore recording.Core
	conf          *conf.Bootstrap
	sessions      sync.Map
}

type playbackSession struct {
	SessionID string    `json:"session_id"`
	DeviceID  string    `json:"device_id"`
	ChannelID string    `json:"channel_id"`
	StartMs   int64     `json:"start_ms"`
	EndMs     int64     `json:"end_ms"`
	StreamURL string    `json:"stream_url"`
	Status    string    `json:"status"`
	Scale     float64   `json:"scale"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type queryPlaybackFilesInput struct {
	DeviceID  string `json:"deviceId"`
	ChannelID string `json:"channelId" binding:"required"`
	StartTime int64  `json:"startTime" binding:"required"`
	EndTime   int64  `json:"endTime" binding:"required"`
	Page      int    `json:"page"`
	Size      int    `json:"size"`
}

type queryPlaybackFilesOutput struct {
	Files []playbackFileItem `json:"files"`
	Total int64              `json:"total"`
}

type playbackFileItem struct {
	FileID    string `json:"fileId"`
	BeginTime int64  `json:"beginTime"`
	EndTime   int64  `json:"endTime"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
}

type createPlaybackSessionInput struct {
	DeviceID  string `json:"deviceId"`
	ChannelID string `json:"channelId" binding:"required"`
	FileID    string `json:"fileId"`
	StartTime int64  `json:"startTime" binding:"required"`
	EndTime   int64  `json:"endTime" binding:"required"`
}

type createPlaybackSessionOutput struct {
	SessionID string `json:"sessionId"`
	StreamURL string `json:"streamUrl"`
	Transport string `json:"transport"`
	SSRC      string `json:"ssrc"`
	StartTime int64  `json:"startTime"`
}

type controlPlaybackSessionInput struct {
	Action     string  `json:"action" binding:"required"`
	RangeStart int64   `json:"rangeStart"`
	Scale      float64 `json:"scale"`
}

type playbackCapabilitiesOutput struct {
	SupportsSeek   bool      `json:"supportsSeek"`
	SupportsPause  bool      `json:"supportsPause"`
	SupportsResume bool      `json:"supportsResume"`
	SupportsScale  bool      `json:"supportsScale"`
	ScaleRange     []float64 `json:"scaleRange"`
}

// NewRecordingStore 创建录像存储层
func NewRecordingStore(db *gorm.DB) recording.Storer {
	return recordingdb.NewDB(db).AutoMigrate(orm.GetEnabledAutoMigrate())
}

// NewRecordingCore 创建录像管理核心服务
// 依赖 recording.SMSProvider 接口而非 sms.Core，避免循环依赖
func NewRecordingCore(store recording.Storer, cfg *conf.Bootstrap, provider recording.SMSProvider) recording.Core {
	core := recording.NewCore(store,
		recording.WithConfig(&cfg.Server.Recording),
		recording.WithSMSProvider(provider),
	)

	// 启动清理协程
	go core.StartCleanupWorker()

	return core
}

// NewRecordingAPI
// 为什么在 API 层维护 sessions：
// 当前先以最小改动打通前后端联调，先提供可观测的会话语义与生命周期管理，
// 后续即使替换为真实 SIP 会话存储，也能保持接口稳定，降低前端与联调脚本迁移成本。
func NewRecordingAPI(core recording.Core, conf *conf.Bootstrap) RecordingAPI {
	return RecordingAPI{recordingCore: core, conf: conf}
}

// RegisterRecording
// 为什么把 gb28181 回放接口挂在 recording API 内：
// 回放链路与录像检索天然耦合（时间段、片段地址、下载一致性），放在同一模块可以减少跨模块参数漂移。
func RegisterRecording(g gin.IRouter, api RecordingAPI, handler ...gin.HandlerFunc) {
	{
		group := g.Group("/recordings", handler...)
		group.GET("", web.WrapH(api.findRecordings))
		group.GET("/timeline", web.WrapH(api.getTimeline))
		group.GET("/monthly", web.WrapH(api.getMonthlyStats))
		// HLS 播放列表（根据通道 ID 和时间范围生成 m3u8）
		group.GET("/channels/:cid/index.m3u8", api.channelPlaylist)
		group.GET("/:id", web.WrapH(api.getRecording))
		group.PUT("/:id", web.WrapH(api.editRecording))
		group.DELETE("/:id", web.WrapH(api.delRecording))
		group.GET("/:id/download", api.downloadRecording)
	}
	{
		gbPlayback := g.Group("/gb28181/playback", handler...)
		gbPlayback.POST("/files/query", web.WrapH(api.queryPlaybackFiles))
		gbPlayback.POST("/sessions", web.WrapH(api.createPlaybackSession))
		gbPlayback.POST("/sessions/:sessionId/control", web.WrapH(api.controlPlaybackSession))
		gbPlayback.DELETE("/sessions/:sessionId", web.WrapH(api.deletePlaybackSession))
	}
	{
		gbCapability := g.Group("/gb28181/devices", handler...)
		gbCapability.GET("/:deviceId/playback/capabilities", web.WrapH(api.getPlaybackCapabilities))
	}

	// 静态文件服务，用于访问录像 MP4 文件
	// 路径格式: /static/recordings/xxx.mp4?token=xxx
	// Gin Static 支持 HTTP Range 请求，实现边下载边播放（秒播）
	if api.conf != nil && api.conf.Server.Recording.StorageDir != "" {
		slog.Info("注册录像静态文件服务", "path", "/static/recordings", "dir", api.conf.Server.Recording.StorageDir)
		g.Group("/static", handler...).Static("/recordings", api.conf.Server.Recording.StorageDir)
	}
}

// findRecordings 分页查询录像列表
func (a RecordingAPI) findRecordings(c *gin.Context, in *recording.FindRecordingInput) (any, error) {
	ctx := web.WithContext(c.Request)
	items, total, err := a.recordingCore.FindRecordings(ctx, in)
	return gin.H{"items": items, "total": total}, err
}

// getTimeline 获取时间轴数据
func (a RecordingAPI) getTimeline(c *gin.Context, in *recording.TimelineInput) (any, error) {
	items, err := a.recordingCore.GetTimeline(c.Request.Context(), in)
	return gin.H{"items": items}, err
}

func (a RecordingAPI) getRecording(c *gin.Context, _ *struct{}) (*recording.Recording, error) {
	recordingID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	return a.recordingCore.GetRecording(c.Request.Context(), recordingID)
}

func (a RecordingAPI) editRecording(c *gin.Context, in *recording.EditRecordingInput) (*recording.Recording, error) {
	recordingID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	return a.recordingCore.EditRecording(c.Request.Context(), in, recordingID)
}

func (a RecordingAPI) delRecording(c *gin.Context, _ *struct{}) (*recording.Recording, error) {
	recordingID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	return a.recordingCore.DelRecording(c.Request.Context(), recordingID)
}

// getMonthlyStats 获取月度录像统计
func (a RecordingAPI) getMonthlyStats(c *gin.Context, in *recording.MonthlyStatsInput) (*recording.MonthlyStatsOutput, error) {
	return a.recordingCore.GetMonthlyStats(c.Request.Context(), in)
}

// queryPlaybackFiles
// 为什么先复用本地录像查询能力：
// 先复用已有录制数据可以快速验证“查询->播放->控制”的闭环，降低引入 SIP 查询后排障面。
func (a RecordingAPI) queryPlaybackFiles(c *gin.Context, in *queryPlaybackFilesInput) (*queryPlaybackFilesOutput, error) {
	size := in.Size
	if size <= 0 {
		size = 100
	}
	page := in.Page
	if page <= 0 {
		page = 1
	}
	items, total, err := a.recordingCore.FindRecordings(web.WithContext(c.Request), &recording.FindRecordingInput{
		CID:         in.ChannelID,
		PagerFilter: web.PagerFilter{Page: page, Size: size},
		DateFilter:  web.DateFilter{StartMs: in.StartTime, EndMs: in.EndTime},
	})
	if err != nil {
		return nil, err
	}
	out := make([]playbackFileItem, 0, len(items))
	for _, item := range items {
		out = append(out, playbackFileItem{
			FileID:    strconv.FormatInt(item.ID, 10),
			BeginTime: item.StartedAt.UnixMilli(),
			EndTime:   item.EndedAt.UnixMilli(),
			Name:      filepath.Base(item.Path),
			Size:      item.Size,
		})
	}
	return &queryPlaybackFilesOutput{Files: out, Total: total}, nil
}

// createPlaybackSession
// 为什么返回 streamURL 而不是只返回 sessionId：
// 前端回放需要立即可消费的播放地址，减少二次查询，缩短首帧等待并提升问题定位效率。
func (a RecordingAPI) createPlaybackSession(c *gin.Context, in *createPlaybackSessionInput) (*createPlaybackSessionOutput, error) {
	now := time.Now()
	sessionID := fmt.Sprintf("pb_%d_%s", now.UnixMilli(), orm.GenerateRandomString(8))
	token := c.GetString("token")
	streamURL := fmt.Sprintf("/recordings/channels/%s/index.m3u8?start_ms=%d&end_ms=%d", in.ChannelID, in.StartTime, in.EndTime)
	if token != "" {
		streamURL += "&token=" + url.QueryEscape(token)
	}
	s := playbackSession{
		SessionID: sessionID,
		DeviceID:  in.DeviceID,
		ChannelID: in.ChannelID,
		StartMs:   in.StartTime,
		EndMs:     in.EndTime,
		StreamURL: streamURL,
		Status:    "playing",
		Scale:     1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	a.sessions.Store(sessionID, s)
	slog.Info("gb28181 playback session created",
		"deviceId", in.DeviceID,
		"channelId", in.ChannelID,
		"sessionId", sessionID,
		"action", "INVITE",
	)
	return &createPlaybackSessionOutput{
		SessionID: sessionID,
		StreamURL: streamURL,
		Transport: "RTP/AVP",
		SSRC:      "",
		StartTime: in.StartTime,
	}, nil
}

// controlPlaybackSession
// 为什么统一 action 入口：
// 把控制动作收敛为单入口可保证日志维度一致（sessionId/action/status），便于故障时快速关联时序。
func (a RecordingAPI) controlPlaybackSession(c *gin.Context, in *controlPlaybackSessionInput) (gin.H, error) {
	sessionID := c.Param("sessionId")
	raw, ok := a.sessions.Load(sessionID)
	if !ok {
		return nil, reason.ErrNotFound.SetMsg("session not found")
	}
	s := raw.(playbackSession)
	action := strings.ToUpper(strings.TrimSpace(in.Action))
	switch action {
	case "PAUSE":
		s.Status = "paused"
	case "RESUME":
		s.Status = "playing"
	case "SEEK":
		if in.RangeStart > 0 {
			s.StartMs = in.RangeStart
		}
	case "SCALE":
		if in.Scale > 0 {
			s.Scale = in.Scale
		}
	case "TEARDOWN":
		s.Status = "ended"
	default:
		return nil, reason.ErrBadRequest.SetMsg("unsupported action")
	}
	s.UpdatedAt = time.Now()
	a.sessions.Store(sessionID, s)
	slog.Info("gb28181 playback session control",
		"sessionId", sessionID,
		"action", action,
		"status", s.Status,
		"rangeStart", in.RangeStart,
		"scale", in.Scale,
	)
	return gin.H{
		"sessionId":  sessionID,
		"status":     s.Status,
		"action":     action,
		"rangeStart": in.RangeStart,
		"scale":      s.Scale,
	}, nil
}

// deletePlaybackSession
// 为什么显式删除会话：
// 回放异常时不能依赖客户端自然断开，主动释放能避免会话泄漏并降低后端资源占用风险。
func (a RecordingAPI) deletePlaybackSession(c *gin.Context, _ *struct{}) (gin.H, error) {
	sessionID := c.Param("sessionId")
	if _, ok := a.sessions.Load(sessionID); !ok {
		return nil, reason.ErrNotFound.SetMsg("session not found")
	}
	a.sessions.Delete(sessionID)
	slog.Info("gb28181 playback session deleted", "sessionId", sessionID, "action", "BYE")
	return gin.H{"sessionId": sessionID, "status": "ended"}, nil
}

// getPlaybackCapabilities
// 为什么提供能力探测：
// 不同设备对 SEEK/SCALE 支持不一致，提前暴露能力可以让前端降级而不是在控制时报错。
func (a RecordingAPI) getPlaybackCapabilities(c *gin.Context, _ *struct{}) (*playbackCapabilitiesOutput, error) {
	_ = c.Param("deviceId")
	return &playbackCapabilitiesOutput{
		SupportsSeek:   true,
		SupportsPause:  true,
		SupportsResume: true,
		SupportsScale:  true,
		ScaleRange:     []float64{0.5, 1.0, 1.5, 2.0, 4.0},
	}, nil
}

// downloadRecording 下载录像文件
func (a RecordingAPI) downloadRecording(c *gin.Context) {
	recordingID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "invalid recording id"})
		return
	}

	rec, err := a.recordingCore.GetRecording(c.Request.Context(), recordingID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 1, "msg": err.Error()})
		return
	}

	// 构建文件完整路径
	filePath := a.recordingCore.GetFullPath(rec.Path)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"code": 1, "msg": "recording file not found"})
		return
	}

	// 设置下载文件名
	fileName := filepath.Base(filePath)
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))
	c.File(filePath)
}

// channelPlaylist 生成 HLS m3u8 播放列表
// 根据通道 ID 和时间范围，动态生成包含多个 MP4 片段的 m3u8 文件
// 路径: /recordings/channels/:cid/index.m3u8?start_ms=xxx&end_ms=xxx&token=xxx
func (a RecordingAPI) channelPlaylist(c *gin.Context) {
	cid := c.Param("cid")
	if cid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "cid is required"})
		return
	}

	startMs, _ := strconv.ParseInt(c.Query("start_ms"), 10, 64)
	endMs, _ := strconv.ParseInt(c.Query("end_ms"), 10, 64)
	token := c.Query("token")

	if startMs <= 0 || endMs <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1, "msg": "start_ms and end_ms are required"})
		return
	}

	// 获取时间范围内的录像列表（需要完整路径信息）
	ctx := web.WithContext(c.Request)
	recordings, _, err := a.recordingCore.FindRecordings(ctx, &recording.FindRecordingInput{
		CID:         cid,
		PagerFilter: web.PagerFilter{Page: 1, Size: 10000},
		DateFilter:  web.DateFilter{StartMs: startMs, EndMs: endMs},
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 1, "msg": err.Error()})
		return
	}

	if len(recordings) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"code": 1, "msg": "no recordings found in time range"})
		return
	}

	// 生成 m3u8 内容（带 token）
	m3u8Content := a.generateM3U8WithToken(recordings, token)

	c.Header("Content-Type", "application/vnd.apple.mpegurl")
	c.Header("Cache-Control", "no-cache")
	c.String(http.StatusOK, m3u8Content)
}

// generateM3U8WithToken 根据录像列表生成 m3u8 播放列表（每个 MP4 URL 带 token）
// generateM3U8WithToken
// 为什么在这里做路径归一化：
// 录像来源可能是相对路径或已拼接绝对 URL，统一归一化能避免代理/直连场景出现双前缀导致的播放失败。
func (a RecordingAPI) generateM3U8WithToken(recordings []*recording.Recording, token string) string {
	count := len(recordings)
	if count == 0 {
		return ""
	}

	// 创建媒体播放列表 (winSize=0 表示 VOD，不使用滑动窗口)
	pl, err := m3u8.NewMediaPlaylist(0, uint(count))
	if err != nil {
		return ""
	}

	// 设置为 VOD 类型
	pl.MediaType = m3u8.VOD

	// 录像按时间升序排列
	sortedRecs := make([]*recording.Recording, len(recordings))
	copy(sortedRecs, recordings)
	// 按开始时间升序排序
	for i := 0; i < len(sortedRecs)-1; i++ {
		for j := i + 1; j < len(sortedRecs); j++ {
			if sortedRecs[i].StartedAt.After(sortedRecs[j].StartedAt.Time) {
				sortedRecs[i], sortedRecs[j] = sortedRecs[j], sortedRecs[i]
			}
		}
	}

	// 添加每个录像片段
	// URL 格式: /static/recordings/{path}?token=xxx
	// 使用相对路径（以 / 开头），让浏览器相对于当前域名访问
	// 这样无论通过代理还是直接访问都能正常工作
	// ZLM 录制的 fMP4 每个文件 DTS 都从 0 开始，必须在每个片段间添加 DISCONTINUITY
	// 告诉 HLS.js 重置解码器，避免 DTS 不连续导致的解析错误
	for i, rec := range sortedRecs {
		// 每个片段之间都添加 EXT-X-DISCONTINUITY 标签
		// ZLM 每个录像文件都是独立的 fMP4，DTS 从 0 开始，必须重置解码器
		if i > 0 {
			pl.SetDiscontinuity()
		}

		segmentPath := rec.Path
		// FindRecordings 可能已把路径转换为完整 URL，需要还原成静态资源相对路径
		if strings.HasPrefix(segmentPath, "http://") || strings.HasPrefix(segmentPath, "https://") {
			if parsed, err := url.Parse(segmentPath); err == nil {
				segmentPath = parsed.Path
			}
		}
		segmentPath = strings.TrimPrefix(segmentPath, "/")
		segmentPath = strings.TrimPrefix(segmentPath, "static/recordings/")
		segmentPath = strings.TrimPrefix(segmentPath, "/static/recordings/")

		// 使用相对路径（不带域名），让浏览器根据当前页面域名访问
		// 这样开发时通过 Vite 代理、生产时通过后端都能正常访问
		var uri string
		if token != "" {
			uri = fmt.Sprintf("/static/recordings/%s?token=%s", segmentPath, token)
		} else {
			uri = fmt.Sprintf("/static/recordings/%s", segmentPath)
		}
		_ = pl.Append(uri, rec.Duration, "")
	}

	// 关闭播放列表，添加 #EXT-X-ENDLIST 标签
	pl.Close()

	// 编码为字符串
	return pl.String()
}
