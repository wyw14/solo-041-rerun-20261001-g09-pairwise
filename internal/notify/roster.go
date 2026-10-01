package notify

import (
	"github.com/wyw14/cry-97/internal/model"
)

// Roster 维护“处理线 × 值班层级 → 值班人员”的映射。
// 某条处理线未单独排班时回退到默认花名册。
type Roster struct {
	defaults map[model.DutyLevel][]model.DutyContact
	lines    map[model.LineID]map[model.DutyLevel][]model.DutyContact
}

func NewRoster() *Roster {
	return &Roster{
		defaults: make(map[model.DutyLevel][]model.DutyContact),
		lines:    make(map[model.LineID]map[model.DutyLevel][]model.DutyContact),
	}
}

// SetDefault 设置所有处理线共用的默认值班人员。
func (r *Roster) SetDefault(level model.DutyLevel, contacts ...model.DutyContact) {
	r.defaults[level] = cloneContacts(contacts)
}

// Assign 为指定处理线的指定层级排班，覆盖默认名单。
func (r *Roster) Assign(lineID model.LineID, level model.DutyLevel, contacts ...model.DutyContact) {
	byLevel, ok := r.lines[lineID]
	if !ok {
		byLevel = make(map[model.DutyLevel][]model.DutyContact)
		r.lines[lineID] = byLevel
	}
	byLevel[level] = cloneContacts(contacts)
}

// Contacts 返回某条处理线在某层级的值班人员；无专属排班时回退默认名单。
func (r *Roster) Contacts(lineID model.LineID, level model.DutyLevel) []model.DutyContact {
	if byLevel, ok := r.lines[lineID]; ok {
		if contacts, ok := byLevel[level]; ok && len(contacts) > 0 {
			return cloneContacts(contacts)
		}
	}
	return cloneContacts(r.defaults[level])
}

func cloneContacts(in []model.DutyContact) []model.DutyContact {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.DutyContact, len(in))
	copy(out, in)
	return out
}

// DefaultRoster 按处理线生成占位值班名单，Target 模拟各类通知通道。
// 生产部署时应由配置/排班系统替换。
func DefaultRoster(lineIDs []model.LineID) *Roster {
	roster := NewRoster()
	for _, lineID := range lineIDs {
		roster.Assign(lineID, model.LevelOperator,
			model.DutyContact{LineID: lineID, Level: model.LevelOperator, Name: string(lineID) + " 一线值班员", Target: "sms:operator-" + string(lineID)},
		)
		roster.Assign(lineID, model.LevelSupervisor,
			model.DutyContact{LineID: lineID, Level: model.LevelSupervisor, Name: string(lineID) + " 值班主管", Target: "sms:supervisor-" + string(lineID)},
			model.DutyContact{LineID: lineID, Level: model.LevelSupervisor, Name: string(lineID) + " 值班副主管", Target: "im:supervisor-" + string(lineID)},
		)
		roster.Assign(lineID, model.LevelManager,
			model.DutyContact{LineID: lineID, Level: model.LevelManager, Name: "运行经理(总值班)", Target: "call:manager-oncall"},
		)
	}
	return roster
}
