package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// DutyLevel 是值班升级链上的层级，数值越大层级越高。
type DutyLevel string

const (
	LevelOperator   DutyLevel = "operator"   // 一线值班员
	LevelSupervisor DutyLevel = "supervisor" // 值班主管
	LevelManager    DutyLevel = "manager"    // 运行经理
)

// DeliveryStatus 描述一次升级通知的投递结果。
type DeliveryStatus string

const (
	// DeliveryPending 已生成通知、正在投递（时间线查询可能短暂观察到该状态）。
	DeliveryPending DeliveryStatus = "pending"
	// DeliverySent 投递器确认送达。
	DeliverySent DeliveryStatus = "sent"
	// DeliveryFailed 投递器返回失败，错误保存在 DeliveryError，升级链仍按计划继续上行。
	DeliveryFailed DeliveryStatus = "failed"
	// DeliverySuppressed 通知已生成但投递前报警被确认或恢复，升级链先行终止，
	// 因而未实际发出。事件与触发依据仍保留在时间线中供审计。
	DeliverySuppressed DeliveryStatus = "suppressed"
)

// EscalationReason 记录升级被触发的依据类别。
type EscalationReason string

const (
	// ReasonRaised 报警初次产生，按严重度路由到起始值班层。
	ReasonRaised EscalationReason = "raised"
	// ReasonTimeout 报警持续未确认达到该层级等待阈值，升级到下一层。
	ReasonTimeout EscalationReason = "timeout"
)

// 升级链终止原因。
const (
	ChainActive       = ""             // 仍在升级中
	ChainAcknowledged = "acknowledged" // 报警被确认，停止后续升级
	ChainRecovered    = "recovered"    // 报警恢复，停止后续升级
	ChainTopLevel     = "top_level"    // 已通知到最高值班层
)

// EscalationEvent 是升级时间线上的一次通知记录，不可变：
// 它同时保留“为什么触发”（Reason+Basis）和“通知是否送到”（Status）。
type EscalationEvent struct {
	ID       uuid.UUID        `json:"id"`
	AlarmID  uuid.UUID        `json:"alarm_id"`
	LineID   LineID           `json:"line_id"`
	Severity AlarmSeverity    `json:"severity"`
	Level    DutyLevel        `json:"level"`
	Reason   EscalationReason `json:"reason"`
	// Basis 保存触发时的规则判定快照，作为人工审计的触发依据。
	Basis      EscalationBasis `json:"basis"`
	Recipients []DutyContact   `json:"recipients"`
	Status     DeliveryStatus  `json:"status"`
	// DeliveryErr 在投递失败时保留投递器返回的错误。
	DeliveryErr string    `json:"delivery_error,omitempty"`
	NotifiedAt  time.Time `json:"notified_at"`
}

// EscalationBasis 是触发升级时的规则判定快照。时长以毫秒序列化，
// 避免 time.Duration 默认输出纳秒整数。
type EscalationBasis struct {
	TriggeredAt         time.Time `json:"triggered_at"`
	RaisedAt            time.Time `json:"raised_at"`
	DurationMillis      int64     `json:"duration_millis"`
	Acknowledged        bool      `json:"acknowledged"`
	PreviousLevel       DutyLevel `json:"previous_level,omitempty"`
	WaitThresholdMillis int64     `json:"wait_threshold_millis,omitempty"`
	Rule                string    `json:"rule"`
}

// DutyContact 是某条处理线上某一层级的值班人员。
type DutyContact struct {
	LineID LineID    `json:"line_id"`
	Level  DutyLevel `json:"level"`
	Name   string    `json:"name"`
	Target string    `json:"target"`
}

// EscalationTimeline 是一条报警的完整升级时间线，包含每次通知记录
// 以及升级链当前的终止状态。
type EscalationTimeline struct {
	AlarmID         uuid.UUID         `json:"alarm_id"`
	LineID          LineID            `json:"line_id"`
	Severity        AlarmSeverity     `json:"severity"`
	CurrentLevel    DutyLevel         `json:"current_level,omitempty"`
	Acknowledged    bool              `json:"acknowledged"`
	Active          bool              `json:"active"`
	Terminated      bool              `json:"terminated"`
	TerminateReason string            `json:"terminate_reason,omitempty"`
	TerminatedAt    *time.Time        `json:"terminated_at,omitempty"`
	Events          []EscalationEvent `json:"events"`
}

// ErrEscalationNotFound 表示查询不到指定报警的升级记录。
var ErrEscalationNotFound = errors.New("escalation timeline is not found")
