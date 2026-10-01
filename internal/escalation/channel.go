package escalation

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

// Notification is one delivery attempt to one contact.
type Notification struct {
	ID          uuid.UUID      `json:"id"`
	AlarmID     uuid.UUID      `json:"alarm_id"`
	Tier        int            `json:"tier"`
	TierLabel   string         `json:"tier_label"`
	Contact     Contact        `json:"contact"`
	Reason      string         `json:"reason"`
	Status      DeliveryStatus `json:"status"`
	Attempts    int            `json:"attempts"`
	Error       string         `json:"error,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	DeliveredAt *time.Time     `json:"delivered_at,omitempty"`
}

// EventKind tags timeline entries.
type EventKind string

const (
	EventRaised         EventKind = "raised"
	EventNotified       EventKind = "notified"
	EventDelivered      EventKind = "delivered"
	EventRetryScheduled EventKind = "retry_scheduled"
	EventAcknowledged   EventKind = "acknowledged"
	EventRecovered      EventKind = "recovered"
	EventSuppressed     EventKind = "suppressed"
)

// Event is one point on an alarm's escalation timeline.
type Event struct {
	At     time.Time `json:"at"`
	Kind   EventKind `json:"kind"`
	Tier   int       `json:"tier,omitempty"`
	Reason string    `json:"reason,omitempty"`
	Detail string    `json:"detail,omitempty"`
	// Notification, when set, carries the delivery status of this entry.
	Notification *Notification `json:"notification,omitempty"`
}

// Channel delivers a notification to one contact. The identifier selects an
// implementation (sms, email, log, ...).
type Channel interface {
	ID() string
	Send(ctx context.Context, alarm model.Alarm, tier Tier, contact Contact, reason string, at time.Time) error
}

// LogChannel records every send in memory. It is the default channel and is
// handy in tests; configured failures let tests exercise retry behaviour.
type LogChannel struct {
	mu       sync.Mutex
	sent     []LoggedMessage
	failWith map[string]error
}

// LoggedMessage is one message accepted by LogChannel.
type LoggedMessage struct {
	At      time.Time
	Alarm   model.Alarm
	Tier    Tier
	Contact Contact
	Reason  string
}

func NewLogChannel() *LogChannel {
	return &LogChannel{failWith: make(map[string]error)}
}

func (c *LogChannel) ID() string { return "log" }

func (c *LogChannel) Send(_ context.Context, alarm model.Alarm, tier Tier, contact Contact, reason string, at time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err, ok := c.failWith[contact.Target]; ok {
		return err
	}
	c.sent = append(c.sent, LoggedMessage{At: at.UTC(), Alarm: alarm, Tier: tier, Contact: contact, Reason: reason})
	return nil
}

// FailNext marks a target as failing until ClearFailure is called.
func (c *LogChannel) FailNext(target string, err error) {
	c.mu.Lock()
	c.failWith[target] = err
	c.mu.Unlock()
}

func (c *LogChannel) ClearFailure(target string) {
	c.mu.Lock()
	delete(c.failWith, target)
	c.mu.Unlock()
}

// Sent returns a copy of all successfully delivered messages.
func (c *LogChannel) Sent() []LoggedMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]LoggedMessage, len(c.sent))
	copy(out, c.sent)
	return out
}
