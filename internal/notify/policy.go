package notify

import (
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

// Policy 定义分级通知路由规则：
//   - StartLevel：该严重度报警的起始通知层级（critical 跳过一线）；
//   - Timeout：从“上一层通知发出”起，报警持续未确认多久后升级到再下一层；
//   - Chain：升级链（operator → supervisor → manager）。
//
// 确认状态是升级的硬门槛：只要报警已确认，无论持续多久都不再升级。
type Policy struct {
	StartLevel map[model.AlarmSeverity]model.DutyLevel
	Timeout    map[model.AlarmSeverity][]time.Duration
	Chain      []model.DutyLevel
}

// DefaultPolicy 的延迟较短以便演示：警告类 30s/30s 升级，危急类起始即主管、
// 20s 后升级经理。实际部署由配置覆盖。
func DefaultPolicy() Policy {
	return Policy{
		StartLevel: map[model.AlarmSeverity]model.DutyLevel{
			model.SeverityWarning:  model.LevelOperator,
			model.SeverityCritical: model.LevelSupervisor,
		},
		Timeout: map[model.AlarmSeverity][]time.Duration{
			model.SeverityWarning:  {30 * time.Second, 30 * time.Second},
			model.SeverityCritical: {20 * time.Second},
		},
		Chain: []model.DutyLevel{model.LevelOperator, model.LevelSupervisor, model.LevelManager},
	}
}

// EntryFor 返回严重度对应的超时序列（按 Chain 中相邻层级的跃迁编号索引）。
func (p Policy) TimeoutsFor(severity model.AlarmSeverity) []time.Duration {
	return p.Timeout[severity]
}
