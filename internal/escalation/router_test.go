package escalation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func testFixture(t *testing.T, retryDelay time.Duration) (*Router, *LogChannel, *fakeClock) {
	t.Helper()
	defaultRoute := Route{
		Tiers: []Tier{
			{Level: 1, Label: "operator", Contacts: []Contact{
				{Name: "Op One", Channel: "log", Target: "op1"},
			}},
			{Level: 2, Label: "shift-lead", Contacts: []Contact{
				{Name: "Lead One", Channel: "log", Target: "lead1"},
			}},
			{Level: 3, Label: "plant-manager", Contacts: []Contact{
				{Name: "Boss One", Channel: "log", Target: "boss1"},
			}},
		},
	}
	specialRoute := Route{
		LineID: "line-9",
		Tiers: []Tier{
			{Level: 1, Label: "specialist", Contacts: []Contact{
				{Name: "Spec One", Channel: "log", Target: "spec1"},
			}},
		},
	}
	roster, err := NewRoster(defaultRoute, specialRoute)
	if err != nil {
		t.Fatalf("roster: %v", err)
	}
	policy, err := NewPolicy(map[model.AlarmSeverity][]time.Duration{
		model.SeverityWarning:  {2 * time.Minute, 10 * time.Minute},
		model.SeverityCritical: {30 * time.Second, 2 * time.Minute},
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	channel := NewLogChannel()
	clock := &fakeClock{t: time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)}
	router, err := NewRouter(roster, policy, []Channel{channel}, clock.now)
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	router.retryDelay = retryDelay
	return router, channel, clock
}

func targetCounts(messages []LoggedMessage) map[string]int {
	counts := make(map[string]int)
	for _, message := range messages {
		counts[message.Contact.Target]++
	}
	return counts
}

func TestCriticalAlarmEscalatesTierByTier(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	alarm1 := model.NewAlarm("line-1", "PUMP_DOWN", "pump fault", model.SeverityCritical, clock.t)

	router.AlarmRaised(alarm1)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	// Repeated ticks at the same instant must not send duplicates.
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent()); got["op1"] != 1 || len(got) != 1 {
		t.Fatalf("after raise: %v, want only op1 once", got)
	}

	clock.advance(29 * time.Second)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent()); len(got) != 1 {
		t.Fatalf("before 30s deadline: %v, want no tier 2", got)
	}

	clock.advance(time.Second) // 30s unacknowledged -> tier 2
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent()); got["lead1"] != 1 {
		t.Fatalf("at 30s: %v, want lead1 once", got)
	}

	clock.advance(90 * time.Second) // 2m unacknowledged -> tier 3
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	counts := targetCounts(channel.Sent())
	if counts["boss1"] != 1 || counts["op1"] != 1 || counts["lead1"] != 1 {
		t.Fatalf("at 2m: %v, want one message per tier", counts)
	}

	// The chain has topped out; time must not produce further messages.
	clock.advance(time.Hour)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := len(channel.Sent()); got != 3 {
		t.Fatalf("after topping out: %d messages, want 3", got)
	}

	summary := router.Escalations()[0]
	if summary.State != StateFiring || summary.CurrentTier != 3 || summary.NextEscalationAt != nil {
		t.Fatalf("summary = %+v, want firing at tier 3 with nothing scheduled", summary)
	}
}

func TestWarningUsesSlowerPolicy(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	router.AlarmRaised(model.NewAlarm("line-1", "BLANKET_HIGH", "high", model.SeverityWarning, clock.t))

	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	clock.advance(30 * time.Second)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent()); len(got) != 1 {
		t.Fatalf("critical deadline must not apply to warning: %v", got)
	}

	clock.advance(90 * time.Second) // 2m -> tier 2
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent())["lead1"]; got != 1 {
		t.Fatalf("want lead1 at warning deadline, got %v", targetCounts(channel.Sent()))
	}
}

func TestFailedDeliveryRetriesWithoutSkippingOrDuplicates(t *testing.T) {
	router, channel, clock := testFixture(t, 30*time.Second)
	channel.FailNext("op1", errors.New("sms gateway down"))
	router.AlarmRaised(model.NewAlarm("line-1", "BLANKET_HIGH", "high", model.SeverityWarning, clock.t))

	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(channel.Sent()) != 0 {
		t.Fatal("failed channel must not record a delivery")
	}
	// Before the retry delay elapses, nothing is retried.
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	summary := router.Escalations()[0]
	if summary.CurrentTier != 1 || len(summary.Notifications) != 1 ||
		summary.Notifications[0].Attempts != 1 || summary.Notifications[0].Status != StatusFailed {
		t.Fatalf("summary = %+v, want single failed attempt still at tier 1", summary)
	}

	clock.advance(30 * time.Second)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	summary = router.Escalations()[0]
	if summary.Notifications[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", summary.Notifications[0].Attempts)
	}

	channel.ClearFailure("op1")
	clock.advance(30 * time.Second)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	counts := targetCounts(channel.Sent())
	if counts["op1"] != 1 || counts["lead1"] != 0 {
		t.Fatalf("after recovery: %v, want op1 exactly once and no skipped tier", counts)
	}
	// Warning's tier-2 deadline is 2m; at t+60s the chain must wait quietly.
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent())["op1"]; got != 1 {
		t.Fatalf("op1 notified %d times, want exactly 1", got)
	}
}

func TestAcknowledgementFreezesEscalation(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	alarm1 := model.NewAlarm("line-1", "PUMP_DOWN", "pump fault", model.SeverityCritical, clock.t)
	router.AlarmRaised(alarm1)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	clock.advance(30 * time.Second)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	acknowledged := alarm1.Acknowledge()
	router.AlarmAcknowledged(acknowledged)
	clock.advance(time.Hour)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := targetCounts(channel.Sent())["boss1"]; got != 0 {
		t.Fatalf("acknowledged alarm escalated to manager: %v", targetCounts(channel.Sent()))
	}
	summary := router.Escalations()[0]
	if summary.State != StateAcknowledged || summary.CurrentTier != 2 || summary.NextEscalationAt != nil {
		t.Fatalf("summary = %+v, want frozen at tier 2", summary)
	}
}

func TestAcknowledgeBeforeFirstDispatchDropsPending(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	alarm1 := model.NewAlarm("line-1", "PUMP_DOWN", "pump fault", model.SeverityCritical, clock.t)
	router.AlarmRaised(alarm1)
	router.AlarmAcknowledged(alarm1.Acknowledge())
	clock.advance(time.Hour)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(channel.Sent()) != 0 {
		t.Fatalf("frozen pending notifications were sent: %v", targetCounts(channel.Sent()))
	}
	summary := router.Escalations()[0]
	for _, notification := range summary.Notifications {
		if notification.Status != StatusSkipped {
			t.Fatalf("notification status = %s, want skipped", notification.Status)
		}
	}
}

func TestRecoveryTerminatesChain(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	alarm1 := model.NewAlarm("line-1", "PUMP_DOWN", "pump fault", model.SeverityCritical, clock.t)
	router.AlarmRaised(alarm1)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	clock.advance(30 * time.Second) // tier 2 deadline passes, but alarm clears first
	router.AlarmRecovered(alarm1.Recover(clock.t))
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	clock.advance(time.Hour)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	counts := targetCounts(channel.Sent())
	if counts["op1"] != 1 || counts["lead1"] != 0 || counts["boss1"] != 0 {
		t.Fatalf("recovered chain kept escalating: %v", counts)
	}
	summary := router.Escalations()[0]
	if summary.State != StateRecovered || summary.RecoveredAt == nil {
		t.Fatalf("summary = %+v, want recovered", summary)
	}
}

func TestDuplicateRaisesDoNotFanOut(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	first := model.NewAlarm("line-1", "BLANKET_HIGH", "level 3.10 m", model.SeverityWarning, clock.t)
	router.AlarmRaised(first)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	// Same incident fingerprint with a fresh ID/new measured value: suppressed.
	clock.advance(time.Minute)
	dup := model.NewAlarm("line-1", "BLANKET_HIGH", "level 3.20 m", model.SeverityWarning, clock.t)
	router.AlarmRaised(dup)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(router.Escalations()) != 1 {
		t.Fatal("duplicate raise created a second chain")
	}
	if got := targetCounts(channel.Sent())["op1"]; got != 1 {
		t.Fatalf("duplicate raise produced %d messages, want 1", got)
	}
	timeline, err := router.Timeline(first.ID)
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	found := false
	for _, event := range timeline {
		if event.Kind == EventSuppressed {
			found = true
		}
	}
	if !found {
		t.Fatal("suppression not recorded on timeline")
	}

	// Once the original recovers, a later raise of the same code is a new chain.
	router.AlarmRecovered(first.Recover(clock.t))
	clock.advance(time.Minute)
	router.AlarmRaised(model.NewAlarm("line-1", "BLANKET_HIGH", "level 3.30 m", model.SeverityWarning, clock.t))
	if len(router.Escalations()) != 2 {
		t.Fatal("post-recovery raise should start a new chain")
	}
}

func TestLineSpecificRoute(t *testing.T) {
	router, channel, clock := testFixture(t, time.Minute)
	router.AlarmRaised(model.NewAlarm("line-9", "LINE_FAULT", "fault", model.SeverityCritical, clock.t))
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	counts := targetCounts(channel.Sent())
	if counts["spec1"] != 1 || len(counts) != 1 {
		t.Fatalf("line-9 route = %v, want specialist only", counts)
	}
	clock.advance(time.Hour)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(channel.Sent()) != 1 {
		t.Fatal("single-tier route must keep escalating")
	}
}

func TestTimelineKeepsTriggerBasisAndDeliveryStatus(t *testing.T) {
	router, _, clock := testFixture(t, time.Minute)
	alarm1 := model.NewAlarm("line-1", "PUMP_DOWN", "pump fault", model.SeverityCritical, clock.t)
	router.AlarmRaised(alarm1)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	clock.advance(30 * time.Second)
	if err := router.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	events, err := router.Timeline(alarm1.ID)
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	if len(events) < 4 {
		t.Fatalf("timeline = %d events, want raised + 2 tiers of entries", len(events))
	}
	if events[0].Kind != EventRaised {
		t.Fatalf("first event = %s, want raised", events[0].Kind)
	}
	var sawInitial, sawEscalation bool
	for _, event := range events {
		if event.Kind != EventNotified || event.Notification == nil {
			continue
		}
		if event.Notification.Status != StatusSent || event.Notification.DeliveredAt == nil {
			t.Fatalf("notification not marked delivered: %+v", event.Notification)
		}
		if event.Tier == 1 && event.Reason != "" {
			sawInitial = true
		}
		if event.Tier == 2 && event.Notification.Attempts == 1 {
			sawEscalation = true
		}
	}
	if !sawInitial || !sawEscalation {
		t.Fatalf("timeline missing trigger basis: %+v", events)
	}

	if _, err := router.Timeline(alarm1.ID); err != nil {
		t.Fatalf("timeline lookup: %v", err)
	}
}

func TestPolicyAndRosterValidation(t *testing.T) {
	if _, err := NewPolicy(map[model.AlarmSeverity][]time.Duration{
		model.SeverityWarning: {time.Second, time.Second},
	}); err == nil {
		t.Fatal("equal delays should be rejected")
	}
	if _, err := NewRoster(Route{}); err == nil {
		t.Fatal("empty route should be rejected")
	}
	if _, err := NewRoster(Route{Tiers: []Tier{{Level: 1, Contacts: []Contact{{Name: "x"}}}}}); err == nil {
		t.Fatal("contact without channel/target should be rejected")
	}
}
