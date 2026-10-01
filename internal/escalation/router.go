package escalation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/alarm"
	"github.com/wyw14/cry-97/internal/model"
)

// defaultRetryDelay is how long a failed delivery waits before another try.
const defaultRetryDelay = 5 * time.Second

// Router owns the escalation chain of every alarm. It is an alarm.Hook:
// Raise schedules the first tier, Acknowledge freezes the chain, Recover
// terminates it. Periodic Tick calls perform due escalations and retries.
type Router struct {
	roster     *Roster
	policy     *Policy
	channels   map[string]Channel
	now        func() time.Time
	retryDelay time.Duration

	mu     sync.Mutex
	chains map[uuid.UUID]*chain
}

type attempt struct {
	notification *Notification
	contact      Contact
	tier         Tier
	reason       string
	dueAt        time.Time
}

type chain struct {
	alarm model.Alarm
	state State
	route Route
	// tierCount is the highest tier reachable on this route/policy.
	tierCount int
	// currentTier is the last tier entered, 0 before first dispatch.
	currentTier int
	// pending holds notifications that still need a successful delivery.
	pending map[uuid.UUID]*attempt
	// tierComplete marks tiers whose every contact was delivered; a tier may
	// only be left once this is set, so failed delivery never skips a level.
	tierComplete map[int]bool
	// nextDueAt is when the chain takes its next action (retry or next
	// tier); zero when nothing is scheduled.
	nextDueAt time.Time
	timeline  []Event
}

// NewRouter wires a roster, policy and delivery channels.
func NewRouter(roster *Roster, policy *Policy, channels []Channel, now func() time.Time) (*Router, error) {
	if roster == nil || policy == nil || now == nil {
		return nil, fmt.Errorf("escalation router needs roster, policy and clock")
	}
	if len(channels) == 0 {
		return nil, fmt.Errorf("escalation router needs at least one channel")
	}
	byID := make(map[string]Channel, len(channels))
	for _, channel := range channels {
		byID[channel.ID()] = channel
	}
	return &Router{
		roster: roster, policy: policy, channels: byID, now: now,
		retryDelay: defaultRetryDelay, chains: make(map[uuid.UUID]*chain),
	}, nil
}

// Compile-time check that Router satisfies the alarm hook contract.
var _ alarm.Hook = (*Router)(nil)

// fingerprint identifies duplicate raises of the same incident: same line,
// code and severity. The free-text message (which carries measured values)
// is deliberately excluded.
func fingerprint(a model.Alarm) string {
	return string(a.LineID) + "|" + a.Code + "|" + string(a.Severity)
}

// AlarmRaised starts a new chain or suppresses a duplicate of an open one.
func (r *Router) AlarmRaised(a model.Alarm) {
	now := a.RaisedAt.UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.chains {
		if existing.alarm.Active() && fingerprint(existing.alarm) == fingerprint(a) {
			existing.timeline = append(existing.timeline, Event{
				At: now, Kind: EventSuppressed,
				Detail: fmt.Sprintf("duplicate raise %s suppressed against open alarm %s", a.ID, existing.alarm.ID),
			})
			return
		}
	}
	route := r.roster.RouteFor(a.LineID)
	entry := &chain{
		alarm: a, state: StateFiring, route: route,
		tierCount:    min(len(route.Tiers), r.policy.MaxTier(a.Severity)),
		pending:      make(map[uuid.UUID]*attempt),
		tierComplete: make(map[int]bool),
	}
	entry.timeline = append(entry.timeline, Event{At: now, Kind: EventRaised, Detail: "alarm raised; routed to tier 1"})
	r.chains[a.ID] = entry
	r.scheduleTierLocked(entry, 1, now)
	entry.nextDueAt = r.nextActionLocked(entry, now)
}

// AlarmAcknowledged freezes escalation at the tier currently reached.
func (r *Router) AlarmAcknowledged(a model.Alarm) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.chains[a.ID]
	if !ok || entry.state != StateFiring {
		return
	}
	entry.state = StateAcknowledged
	entry.alarm = a
	entry.nextDueAt = time.Time{}
	at := r.now().UTC()
	r.cancelPendingLocked(entry, at, "escalation frozen by acknowledgement")
	entry.timeline = append(entry.timeline, Event{
		At: at, Kind: EventAcknowledged, Tier: entry.currentTier,
		Detail: fmt.Sprintf("alarm acknowledged at tier %d; further escalation stopped", entry.currentTier),
	})
}

// AlarmRecovered terminates escalation outright.
func (r *Router) AlarmRecovered(a model.Alarm) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.chains[a.ID]
	if !ok || entry.state == StateRecovered {
		return
	}
	entry.state = StateRecovered
	entry.alarm = a
	entry.nextDueAt = time.Time{}
	at := r.now().UTC()
	r.cancelPendingLocked(entry, at, "escalation terminated by recovery")
	entry.timeline = append(entry.timeline, Event{
		At: at, Kind: EventRecovered, Tier: entry.currentTier,
		Detail: "alarm recovered; escalation chain terminated",
	})
}

func (r *Router) cancelPendingLocked(entry *chain, at time.Time, reason string) {
	if len(entry.pending) > 0 {
		entry.timeline = append(entry.timeline, Event{
			At: at, Kind: EventNotified, Tier: entry.currentTier,
			Detail: fmt.Sprintf("%s: %d pending notification(s) dropped", reason, len(entry.pending)),
		})
	}
	for _, pending := range entry.pending {
		pending.notification.Status = StatusSkipped
		pending.notification.Error = reason
	}
	entry.pending = make(map[uuid.UUID]*attempt)
}

// scheduleTierLocked creates one pending notification per contact at tier.
// It runs at most once per tier — that is the guard that keeps repeated
// ticks from producing duplicate notifications.
func (r *Router) scheduleTierLocked(entry *chain, level int, now time.Time) {
	tier, ok := entry.route.tier(level)
	if !ok || level > entry.tierCount {
		return
	}
	entry.currentTier = level
	reason := tierReason(entry.alarm, level, now)
	for _, contact := range tier.Contacts {
		notification := &Notification{
			ID: uuid.New(), AlarmID: entry.alarm.ID,
			Tier: level, TierLabel: tier.Label, Contact: contact,
			Reason: reason, Status: StatusPending, CreatedAt: now,
		}
		entry.pending[notification.ID] = &attempt{
			notification: notification, contact: contact, tier: tier,
			reason: reason, dueAt: now,
		}
		entry.timeline = append(entry.timeline, Event{
			At: now, Kind: EventNotified, Tier: level, Reason: reason,
			Detail:       fmt.Sprintf("queued for %s via %s", contact.Name, contact.Channel),
			Notification: notification,
		})
	}
}

func tierReason(a model.Alarm, tier int, now time.Time) string {
	age := now.Sub(a.RaisedAt.UTC()).Round(time.Second)
	if tier == 1 {
		return fmt.Sprintf("initial dispatch: %s alarm %s raised on %s", a.Severity, a.Code, a.LineID)
	}
	return fmt.Sprintf("%s alarm %s on %s unacknowledged for %s; escalating to tier %d",
		a.Severity, a.Code, a.LineID, age, tier)
}

// plan gathers the work due at time `at`: it first enters every policy-due
// tier (previous tier must be fully delivered), then returns all pending
// notifications whose retry/dispatch time has come. Called under lock.
func (r *Router) planLocked(at time.Time) map[*chain][]*attempt {
	due := make(map[*chain][]*attempt)
	for _, entry := range r.chains {
		if entry.state != StateFiring {
			continue
		}
		// Enter every tier whose deadline has passed, as long as the tier
		// below has been delivered in full.
		for {
			if entry.currentTier > 0 && !entry.tierComplete[entry.currentTier] {
				break
			}
			next := entry.currentTier + 1
			if next > entry.tierCount {
				break
			}
			dueAt, ok := r.policy.DueAt(entry.alarm.Severity, entry.alarm.RaisedAt.UTC(), next)
			if !ok || dueAt.After(at) {
				break
			}
			r.scheduleTierLocked(entry, next, at)
		}
		for _, pending := range entry.pending {
			if !pending.dueAt.After(at) {
				due[entry] = append(due[entry], pending)
			}
		}
		entry.nextDueAt = r.nextActionLocked(entry, at)
	}
	return due
}

// nextActionLocked reports when the chain acts next: the earliest of a
// pending retry and the next tier's policy deadline. Zero when neither.
func (r *Router) nextActionLocked(entry *chain, at time.Time) time.Time {
	var earliest time.Time
	for _, pending := range entry.pending {
		if earliest.IsZero() || pending.dueAt.Before(earliest) {
			earliest = pending.dueAt
		}
	}
	if entry.currentTier == 0 || entry.tierComplete[entry.currentTier] {
		next := entry.currentTier + 1
		if next <= entry.tierCount {
			if dueAt, ok := r.policy.DueAt(entry.alarm.Severity, entry.alarm.RaisedAt.UTC(), next); ok {
				if earliest.IsZero() || dueAt.Before(earliest) {
					earliest = dueAt
				}
			}
		}
	}
	if !earliest.IsZero() && earliest.Before(at) {
		earliest = at
	}
	return earliest
}

type delivery struct {
	entry   *chain
	attempt *attempt
	err     error
}

// Tick performs every escalation and retry due at or before the current
// time. It converges in a single call regardless of how much clock time has
// passed, and it is idempotent: repeating it at the same instant sends
// nothing twice, because tiers are entered once and notifications are only
// retried while they lack a successful delivery.
func (r *Router) Tick(ctx context.Context) error {
	for {
		at := r.now().UTC()

		r.mu.Lock()
		planned := r.planLocked(at)
		batch := make([]delivery, 0)
		for entry, attempts := range planned {
			for _, pending := range attempts {
				batch = append(batch, delivery{entry: entry, attempt: pending})
			}
		}
		r.mu.Unlock()

		if len(batch) == 0 {
			return nil
		}

		// Channels run outside the router lock; a slow channel cannot stall
		// alarm state changes.
		results := make([]delivery, len(batch))
		for i, item := range batch {
			item.err = r.deliver(ctx, item.entry.alarm, item.attempt, at)
			results[i] = item
		}

		r.mu.Lock()
		for _, result := range results {
			// Acknowledge/recover may have landed while the channel was busy;
			// late results for frozen chains are discarded.
			if result.entry.state != StateFiring {
				continue
			}
			r.applyDeliveryLocked(result.entry, result.attempt, result.err, at)
		}
		r.mu.Unlock()
	}
}

func entryAlarm(d delivery) model.Alarm { return d.entry.alarm }

func (r *Router) deliver(ctx context.Context, a model.Alarm, pending *attempt, at time.Time) error {
	channel, ok := r.channels[pending.contact.Channel]
	if !ok {
		return fmt.Errorf("no channel %q configured", pending.contact.Channel)
	}
	return channel.Send(ctx, a, pending.tier, pending.contact, pending.reason, at)
}

func (r *Router) applyDeliveryLocked(entry *chain, pending *attempt, sendErr error, at time.Time) {
	n := pending.notification
	n.Attempts++
	if sendErr != nil {
		n.Status = StatusFailed
		n.Error = sendErr.Error()
		pending.dueAt = at.Add(r.retryDelay)
		entry.timeline = append(entry.timeline, Event{
			At: at, Kind: EventRetryScheduled, Tier: n.Tier,
			Detail: fmt.Sprintf("delivery to %s failed (attempt %d): %s; retry at %s",
				n.Contact.Name, n.Attempts, sendErr.Error(), pending.dueAt.Format(time.RFC3339)),
			Notification: n,
		})
		entry.nextDueAt = r.nextActionLocked(entry, at)
		return
	}
	stamp := at
	n.Status = StatusSent
	n.Error = ""
	n.DeliveredAt = &stamp
	entry.timeline = append(entry.timeline, Event{
		At: at, Kind: EventDelivered, Tier: n.Tier,
		Detail:       fmt.Sprintf("delivered to %s via %s on attempt %d", n.Contact.Name, n.Contact.Channel, n.Attempts),
		Notification: n,
	})
	delete(entry.pending, n.ID)
	tierDone := true
	for _, other := range entry.pending {
		if other.notification.Tier == n.Tier {
			tierDone = false
			break
		}
	}
	if tierDone {
		entry.tierComplete[n.Tier] = true
	}
	entry.nextDueAt = r.nextActionLocked(entry, at)
}

// Summary is the queryable status of one alarm's chain.
type Summary struct {
	AlarmID          uuid.UUID           `json:"alarm_id"`
	LineID           model.LineID        `json:"line_id"`
	Code             string              `json:"code"`
	Message          string              `json:"message"`
	Severity         model.AlarmSeverity `json:"severity"`
	State            State               `json:"state"`
	RaisedAt         time.Time           `json:"raised_at"`
	CurrentTier      int                 `json:"current_tier"`
	CurrentTierLabel string              `json:"current_tier_label,omitempty"`
	NextEscalationAt *time.Time          `json:"next_escalation_at,omitempty"`
	Notifications    []Notification      `json:"notifications"`
	RecoveredAt      *time.Time          `json:"recovered_at,omitempty"`
}

// Escalations lists every chain, oldest alarm first.
func (r *Router) Escalations() []Summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Summary, 0, len(r.chains))
	for _, entry := range r.chains {
		out = append(out, r.summarizeLocked(entry))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RaisedAt.Before(out[j].RaisedAt) })
	return out
}

// Timeline returns the ordered escalation timeline of one alarm: the trigger
// basis of every step and the current delivery status of every notification.
func (r *Router) Timeline(alarmID uuid.UUID) ([]Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.chains[alarmID]
	if !ok {
		return nil, fmt.Errorf("escalation timeline is not found")
	}
	events := make([]Event, len(entry.timeline))
	copy(events, entry.timeline)
	return events, nil
}

func (r *Router) summarizeLocked(entry *chain) Summary {
	summary := Summary{
		AlarmID: entry.alarm.ID, LineID: entry.alarm.LineID, Code: entry.alarm.Code,
		Message: entry.alarm.Message, Severity: entry.alarm.Severity, State: entry.state,
		RaisedAt: entry.alarm.RaisedAt, CurrentTier: entry.currentTier,
		RecoveredAt: entry.alarm.RecoveredAt,
	}
	if tier, ok := entry.route.tier(entry.currentTier); ok {
		summary.CurrentTierLabel = tier.Label
	}
	if !entry.nextDueAt.IsZero() {
		next := entry.nextDueAt
		summary.NextEscalationAt = &next
	}
	seen := make(map[uuid.UUID]struct{})
	for _, event := range entry.timeline {
		if event.Notification == nil {
			continue
		}
		id := event.Notification.ID
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		summary.Notifications = append(summary.Notifications, *event.Notification)
	}
	sort.Slice(summary.Notifications, func(i, j int) bool {
		return summary.Notifications[i].CreatedAt.Before(summary.Notifications[j].CreatedAt)
	})
	return summary
}
