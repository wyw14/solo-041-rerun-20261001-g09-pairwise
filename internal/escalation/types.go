// Package escalation routes unresolved alarms up a per-line on-call chain.
//
// An alarm starts at tier 0 of its line roster. While it stays active and
// unacknowledged, every policy deadline moves the notification to the next
// tier. Acknowledgement freezes the chain at the current tier; recovery
// terminates it. Each send records why it fired (severity, age, tier) and how
// delivery went, so the full escalation timeline stays queryable.
package escalation

import (
	"errors"
	"strings"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

// State is the lifecycle state of one alarm's escalation chain.
type State string

const (
	// StateFiring is an active, unacknowledged alarm still being escalated.
	StateFiring State = "firing"
	// StateAcknowledged means somebody took ownership; escalation is frozen.
	StateAcknowledged State = "acknowledged"
	// StateRecovered means the alarm cleared; escalation is terminated.
	StateRecovered State = "recovered"
)

// DeliveryStatus of a single notification.
type DeliveryStatus string

const (
	StatusPending DeliveryStatus = "pending"
	StatusSent    DeliveryStatus = "sent"
	StatusFailed  DeliveryStatus = "failed"
	StatusSkipped DeliveryStatus = "skipped"
)

// Contact is one on-call person reachable through a channel.
type Contact struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Target  string `json:"target"`
}

// Tier is one level of a duty chain; all contacts at a tier are notified
// together before the chain moves on.
type Tier struct {
	Level    int       `json:"level"`
	Label    string    `json:"label"`
	Contacts []Contact `json:"contacts"`
}

// Route is one ordered on-call chain.
type Route struct {
	LineID model.LineID `json:"line_id"`
	Tiers  []Tier       `json:"tiers"`
}

func (r Route) tier(level int) (Tier, bool) {
	for _, tier := range r.Tiers {
		if tier.Level == level {
			return tier, true
		}
	}
	return Tier{}, false
}

// Roster resolves the duty chain for a process line. Lines without an
// explicit route use the default route.
type Roster struct {
	defaultRoute Route
	lineRoutes   map[model.LineID]Route
}

// NewRoster builds a roster. The default route must contain at least one tier.
func NewRoster(defaultRoute Route, lineRoutes ...Route) (*Roster, error) {
	if err := validateRoute(defaultRoute); err != nil {
		return nil, err
	}
	byLine := make(map[model.LineID]Route, len(lineRoutes))
	for _, route := range lineRoutes {
		if err := validateRoute(route); err != nil {
			return nil, err
		}
		byLine[route.LineID] = route
	}
	return &Roster{defaultRoute: defaultRoute, lineRoutes: byLine}, nil
}

func validateRoute(route Route) error {
	if len(route.Tiers) == 0 {
		return errors.New("escalation route needs at least one tier")
	}
	seen := map[int]struct{}{}
	for _, tier := range route.Tiers {
		if tier.Level < 0 {
			return errors.New("escalation tier level must be non-negative")
		}
		if _, dup := seen[tier.Level]; dup {
			return errors.New("escalation tier levels must be unique")
		}
		seen[tier.Level] = struct{}{}
		if len(tier.Contacts) == 0 {
			return errors.New("escalation tier needs at least one contact")
		}
		for _, contact := range tier.Contacts {
			if strings.TrimSpace(contact.Name) == "" || strings.TrimSpace(contact.Channel) == "" ||
				strings.TrimSpace(contact.Target) == "" {
				return errors.New("escalation contact needs name, channel and target")
			}
		}
	}
	return nil
}

// RouteFor returns the configured route for a line, falling back to default.
func (r *Roster) RouteFor(lineID model.LineID) Route {
	if route, ok := r.lineRoutes[lineID]; ok {
		return route
	}
	return r.defaultRoute
}

// Policy maps a severity to the delays after which an unacknowledged alarm
// moves up one tier. Delays are measured from RaisedAt and must be strictly
// increasing. The chain stops once its route runs out of tiers.
type Policy struct {
	delays map[model.AlarmSeverity][]time.Duration
}

// NewPolicy builds a policy. Every configured severity needs at least one
// delay; severity zero means "raise the alarm" and is always allowed.
func NewPolicy(delays map[model.AlarmSeverity][]time.Duration) (*Policy, error) {
	if len(delays) == 0 {
		return nil, errors.New("escalation policy needs at least one severity")
	}
	cloned := make(map[model.AlarmSeverity][]time.Duration, len(delays))
	for severity, values := range delays {
		if len(values) == 0 {
			return nil, errors.New("escalation severity needs at least one delay")
		}
		previous := time.Duration(0)
		for _, value := range values {
			if value <= 0 {
				return nil, errors.New("escalation delay must be positive")
			}
			if value <= previous {
				return nil, errors.New("escalation delays must be strictly increasing")
			}
			previous = value
		}
		cloned[severity] = append([]time.Duration(nil), values...)
	}
	return &Policy{delays: cloned}, nil
}

// DueAt is when the alarm must reach the given tier (1-based). Tier 1 is due
// at raise time; the second boolean is false when the policy never reaches
// that tier (the chain tops out).
func (p *Policy) DueAt(severity model.AlarmSeverity, raisedAt time.Time, tier int) (time.Time, bool) {
	if tier < 1 {
		return time.Time{}, false
	}
	values, ok := p.delays[severity]
	if !ok || tier-1 > len(values) {
		return time.Time{}, false
	}
	if tier == 1 {
		return raisedAt.UTC(), true
	}
	return raisedAt.Add(values[tier-2]).UTC(), true
}

// MaxTier is the highest tier the policy can schedule for a severity.
func (p *Policy) MaxTier(severity model.AlarmSeverity) int {
	return len(p.delays[severity]) + 1
}
