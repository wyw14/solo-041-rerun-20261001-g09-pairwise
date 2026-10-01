package escalation

import (
	"fmt"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

// DefaultPolicy is the out-of-the-box severity schedule. Critical alarms
// climb the chain on 30s / 2min; warnings get more breathing room on
// 2min / 10min. Both top out at tier 3.
func DefaultPolicy() *Policy {
	policy, err := NewPolicy(map[model.AlarmSeverity][]time.Duration{
		model.SeverityWarning:  {2 * time.Minute, 10 * time.Minute},
		model.SeverityCritical: {30 * time.Second, 2 * time.Minute},
	})
	if err != nil {
		panic(err)
	}
	return policy
}

// DefaultRoster builds the duty chain used when no roster is configured.
// Tier 1 is the line operator, tier 2 the on-call shift lead, tier 3 the
// plant manager. All contacts are reached through the "log" channel; real
// deployments swap in SMS/email channels and configure per-line overrides.
func DefaultRoster(lines []model.LineID) *Roster {
	tier := func(level int, label, name, target string) Tier {
		return Tier{
			Level: level, Label: label,
			Contacts: []Contact{{Name: name, Channel: "log", Target: target}},
		}
	}
	lineRoutes := make([]Route, 0, len(lines))
	for _, lineID := range lines {
		lineRoutes = append(lineRoutes, Route{
			LineID: lineID,
			Tiers: []Tier{
				tier(1, "line-operator",
					fmt.Sprintf("operator-%s", lineID), fmt.Sprintf("operator:%s", lineID)),
				tier(2, "shift-lead", "shift-lead-on-call", "shift-lead"),
				tier(3, "plant-manager", "plant-manager-on-call", "plant-manager"),
			},
		})
	}
	// Every line carries the same chain; the default covers unknown lines.
	defaultRoute := Route{Tiers: lineRoutes[0].Tiers}
	roster, err := NewRoster(defaultRoute, lineRoutes...)
	if err != nil {
		panic(err)
	}
	return roster
}
