package notify

import (
	"context"
	"log"

	"github.com/wyw14/cry-97/internal/model"
)

// Notifier 负责把一次升级通知实际投递给值班人员（短信/电话/IM 等）。
// 返回 error 时路由器会把该次投递标记为 failed 并保留错误信息；
// 升级链不会因单次投递失败而中断，下一层仍按策略继续。
type Notifier interface {
	Notify(ctx context.Context, event model.EscalationEvent) error
}

// LogNotifier 是默认投递器：把通知写入日志并始终视为送达。
// 真实部署可替换为短信网关/呼叫中心客户端。
type LogNotifier struct {
	logger *log.Logger
}

func NewLogNotifier(logger *log.Logger) *LogNotifier {
	if logger == nil {
		logger = log.Default()
	}
	return &LogNotifier{logger: logger}
}

func (n *LogNotifier) Notify(_ context.Context, event model.EscalationEvent) error {
	for _, contact := range event.Recipients {
		n.logger.Printf("escalation notify alarm=%s line=%s severity=%s level=%s reason=%s -> %s (%s) %q",
			event.AlarmID, event.LineID, event.Severity, event.Level, event.Reason,
			contact.Name, contact.Target, event.Basis.Rule)
	}
	return nil
}

// FailNotifier 是测试用投递器，按 Target 模拟投递失败。
type FailNotifier struct {
	FailTargets map[string]bool
}

func (n FailNotifier) Notify(_ context.Context, event model.EscalationEvent) error {
	for _, contact := range event.Recipients {
		if n.FailTargets[contact.Target] {
			return errDelivery(contact.Target)
		}
	}
	return nil
}

type deliveryError string

func (e deliveryError) Error() string { return "delivery failed for target " + string(e) }

func errDelivery(target string) error { return deliveryError(target) }
