package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

// recordNotifier 记录每次投递，可按目标模拟失败。
type recordNotifier struct {
	mu        sync.Mutex
	delivered []model.EscalationEvent
	fail      map[string]bool
}

func (n *recordNotifier) Notify(_ context.Context, event model.EscalationEvent) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, c := range event.Recipients {
		if n.fail[c.Target] {
			return errors.New("gateway unreachable: " + c.Target)
		}
	}
	n.delivered = append(n.delivered, event)
	return nil
}

func (n *recordNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.delivered)
}

func (n *recordNotifier) levelsFor(alarmID uuid.UUID) []model.DutyLevel {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []model.DutyLevel
	for _, e := range n.delivered {
		if e.AlarmID == alarmID {
			out = append(out, e.Level)
		}
	}
	return out
}

func newTestRouter(t *testing.T, notifier Notifier) (*Router, *time.Time) {
	t.Helper()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	now := base
	lines := []model.LineID{"line-1", "line-2"}
	router := NewRouter(DefaultRoster(lines), DefaultPolicy(), notifier, func() time.Time { return now })
	return router, &now
}

func warningAlarm(raisedAt time.Time) model.Alarm {
	return model.NewAlarm("line-1", "SETTLING_BLANKET_HIGH", "blanket high", model.SeverityWarning, raisedAt)
}

func criticalAlarm(raisedAt time.Time) model.Alarm {
	return model.NewAlarm("line-1", "PROCESS_EMERGENCY", "emergency", model.SeverityCritical, raisedAt)
}

// 警告类报警：产生即通知一线；30s 未确认升级主管；再过 30s 升级经理并到顶终止。
func TestRouter_WarningEscalatesThroughChain(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)

	if got := notifier.levelsFor(a.ID); len(got) != 1 || got[0] != model.LevelOperator {
		t.Fatalf("raise should notify operator once, got %v", got)
	}
	tl, err := router.Timeline(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if tl.Terminated || tl.CurrentLevel != model.LevelOperator {
		t.Fatalf("fresh alarm chain should be active at operator, got %+v", tl)
	}

	// 未到阈值：不升级。
	*now = now.Add(29 * time.Second)
	if created := router.Tick(context.Background()); len(created) != 0 {
		t.Fatalf("no escalation expected before threshold, got %d events", len(created))
	}

	// 达到第一层阈值：升级主管，触发依据记录持续时长与阈值。
	*now = now.Add(1 * time.Second)
	created := router.Tick(context.Background())
	if len(created) != 1 || created[0].Level != model.LevelSupervisor || created[0].Reason != model.ReasonTimeout {
		t.Fatalf("expected one timeout escalation to supervisor, got %+v", created)
	}
	if created[0].Basis.DurationMillis != int64(30*time.Second/time.Millisecond) {
		t.Fatalf("basis duration = %d, want 30000", created[0].Basis.DurationMillis)
	}
	if created[0].Basis.WaitThresholdMillis != int64(30*time.Second/time.Millisecond) ||
		created[0].Basis.PreviousLevel != model.LevelOperator {
		t.Fatalf("basis missing threshold/previous level: %+v", created[0].Basis)
	}
	if created[0].Status != model.DeliverySent {
		t.Fatalf("delivery status = %s, want sent", created[0].Status)
	}

	// 达到第二层阈值：升级经理，到顶终止。
	*now = now.Add(30 * time.Second)
	created = router.Tick(context.Background())
	if len(created) != 1 || created[0].Level != model.LevelManager {
		t.Fatalf("expected escalation to manager, got %+v", created)
	}
	tl, _ = router.Timeline(a.ID)
	if !tl.Terminated || tl.TerminateReason != model.ChainTopLevel || len(tl.Events) != 3 {
		t.Fatalf("chain should terminate at top level with 3 events, got %+v", tl)
	}

	// 终止后继续 Tick 不应再有任何通知。
	*now = now.Add(10 * time.Minute)
	if created := router.Tick(context.Background()); len(created) != 0 {
		t.Fatalf("terminated chain must not escalate further, got %d", len(created))
	}
}

// 危急类报警跳过一线，起始即主管。
func TestRouter_CriticalStartsAtSupervisor(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := criticalAlarm(*now)
	router.HandleRaised(a)
	if got := notifier.levelsFor(a.ID); len(got) != 1 || got[0] != model.LevelSupervisor {
		t.Fatalf("critical alarm must start at supervisor, got %v", got)
	}
	if tl, _ := router.Timeline(a.ID); tl.Events[0].Reason != model.ReasonRaised {
		t.Fatalf("first event reason = %s, want raised", tl.Events[0].Reason)
	}

	// 19s 不升级；20s 升级经理。
	*now = now.Add(19 * time.Second)
	if created := router.Tick(context.Background()); len(created) != 0 {
		t.Fatalf("critical should wait 20s, escalated after 19s")
	}
	*now = now.Add(1 * time.Second)
	created := router.Tick(context.Background())
	if len(created) != 1 || created[0].Level != model.LevelManager {
		t.Fatalf("expected manager escalation after 20s, got %+v", created)
	}
}

// 重复扫描（含并发）不能刷出重复通知。
func TestRouter_RepeatedTicksAreIdempotent(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)
	*now = now.Add(30 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = router.Tick(context.Background()) }()
	}
	wg.Wait()

	if got := notifier.levelsFor(a.ID); len(got) != 2 || got[0] != model.LevelOperator || got[1] != model.LevelSupervisor {
		t.Fatalf("exactly one supervisor notification expected, got %v", got)
	}
	tl, _ := router.Timeline(a.ID)
	if len(tl.Events) != 2 {
		t.Fatalf("timeline should hold 2 events, got %d", len(tl.Events))
	}

	// 再来若干串行 Tick 仍然不增加事件。
	for i := 0; i < 5; i++ {
		if created := router.Tick(context.Background()); len(created) != 0 {
			t.Fatalf("repeat tick created duplicate events: %d", len(created))
		}
	}
}

// 报警被确认后，升级立即终止，持续再久也不再通知。
func TestRouter_AcknowledgeStopsEscalation(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)
	router.HandleAcknowledged(a.Acknowledge())

	*now = now.Add(10 * time.Minute)
	if created := router.Tick(context.Background()); len(created) != 0 {
		t.Fatalf("acknowledged alarm must not escalate, got %d", len(created))
	}
	tl, _ := router.Timeline(a.ID)
	if !tl.Terminated || tl.TerminateReason != model.ChainAcknowledged || tl.TerminatedAt == nil {
		t.Fatalf("acknowledged chain state wrong: %+v", tl)
	}
	if !tl.Acknowledged {
		t.Fatalf("timeline should reflect acknowledged state")
	}
	// 历史通知保留。
	if len(tl.Events) != 1 {
		t.Fatalf("prior notification must be retained, got %d events", len(tl.Events))
	}
}

// 报警恢复后终止后续升级。
func TestRouter_RecoverStopsEscalation(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)
	*now = now.Add(30 * time.Second)
	router.Tick(context.Background()) // 已到主管

	recoveredAt := now.Add(10 * time.Second)
	router.HandleRecovered(a.Recover(recoveredAt))
	*now = now.Add(10 * time.Minute)
	if created := router.Tick(context.Background()); len(created) != 0 {
		t.Fatalf("recovered alarm must not escalate, got %d", len(created))
	}
	tl, _ := router.Timeline(a.ID)
	if !tl.Terminated || tl.TerminateReason != model.ChainRecovered || tl.Active {
		t.Fatalf("recovered chain state wrong: %+v", tl)
	}
	// 升级过程中已有的两级通知都保留。
	if len(tl.Events) != 2 {
		t.Fatalf("escalation history should be retained, got %d events", len(tl.Events))
	}
}

// 即便不调用恢复钩子，下一轮 Tick 观察到报警已恢复（数据来自存储）也要终止。
func TestRouter_TickObservesRecoveryWithoutHook(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)

	router.mu.Lock()
	router.chains[a.ID].alarm = a.Recover(*now)
	router.mu.Unlock()

	if created := router.Tick(context.Background()); len(created) != 0 {
		t.Fatalf("no events expected once alarm recovered, got %d", len(created))
	}
	tl, _ := router.Timeline(a.ID)
	if !tl.Terminated || tl.TerminateReason != model.ChainRecovered {
		t.Fatalf("tick should terminate recovered chain, got %+v", tl)
	}
}

// 投递失败必须在时间线中保留 failed 状态和错误，且不阻断后续层级升级。
func TestRouter_DeliveryFailureRecordedButChainContinues(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{"sms:operator-line-1": true}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)
	tl, _ := router.Timeline(a.ID)
	first := tl.Events[0]
	if first.Status != model.DeliveryFailed || first.DeliveryErr == "" {
		t.Fatalf("failed delivery must be recorded with error, got %+v", first)
	}
	if notifier.count() != 0 {
		t.Fatal("failed notification must not appear as delivered")
	}

	// 一线投递失败不影响 30s 后升级主管（主管的目标未配置失败）。
	*now = now.Add(30 * time.Second)
	created := router.Tick(context.Background())
	if len(created) != 1 || created[0].Level != model.LevelSupervisor || created[0].Status != model.DeliverySent {
		t.Fatalf("chain should continue to supervisor with sent status, got %+v", created)
	}
}

// 时间线可按报警和按处理线查询，未知报警返回未找到。
func TestRouter_TimelineQueries(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a1 := warningAlarm(*now)
	router.HandleRaised(a1)
	a2 := model.NewAlarm("line-2", "PROCESS_EMERGENCY", "emergency", model.SeverityCritical, *now)
	router.HandleRaised(a2)

	if _, err := router.Timeline(uuid.New()); !errors.Is(err, model.ErrEscalationNotFound) {
		t.Fatalf("unknown alarm should return ErrEscalationNotFound, got %v", err)
	}
	all := router.Timelines("")
	if len(all) != 2 {
		t.Fatalf("all timelines = %d, want 2", len(all))
	}
	line2 := router.Timelines("line-2")
	if len(line2) != 1 || line2[0].AlarmID != a2.ID {
		t.Fatalf("line filter should return only line-2 alarm, got %+v", line2)
	}

	// 时间线包含触发依据与投递对象，供审计查询。
	tl, _ := router.Timeline(a1.ID)
	if len(tl.Events[0].Recipients) != 1 || tl.Events[0].Recipients[0].Target != "sms:operator-line-1" {
		t.Fatalf("recipients missing on timeline: %+v", tl.Events[0].Recipients)
	}
	if tl.Events[0].Basis.Rule == "" || tl.Events[0].Basis.TriggeredAt.IsZero() {
		t.Fatalf("trigger basis should be persisted: %+v", tl.Events[0].Basis)
	}
}

// 同一报警重复进入 HandleRaised 是幂等的。
func TestRouter_DuplicateRaiseIsIgnored(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, _ := newTestRouter(t, notifier)

	a := warningAlarm(time.Now().UTC())
	router.HandleRaised(a)
	router.HandleRaised(a)
	if got := notifier.levelsFor(a.ID); len(got) != 1 {
		t.Fatalf("duplicate raise must not notify twice, got %v", got)
	}
}

// 严重度的处理线路由：通知对象来自对应处理线的花名册，互不串线。
func TestRouter_RoutesByLineRoster(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	lines := []model.LineID{"line-1", "line-2"}
	now := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	router := NewRouter(DefaultRoster(lines), DefaultPolicy(), notifier, func() time.Time { return now })

	a1 := warningAlarm(now)
	router.HandleRaised(a1)
	a2 := model.NewAlarm("line-2", "X", "y", model.SeverityWarning, now)
	router.HandleRaised(a2)

	tl1, _ := router.Timeline(a1.ID)
	tl2, _ := router.Timeline(a2.ID)
	if tl1.Events[0].Recipients[0].LineID != "line-1" || tl2.Events[0].Recipients[0].LineID != "line-2" {
		t.Fatalf("recipients must come from the alarm's own line: %+v %+v",
			tl1.Events[0].Recipients, tl2.Events[0].Recipients)
	}
}

// gateNotifier 第一次 supervisor 投递阻塞在 gate 上，并把被阻塞事件发到 started，
// 其余调用直通。用于在一轮 Tick 的多次投递之间插入恢复操作。
type gateNotifier struct {
	gate    chan struct{}
	started chan model.EscalationEvent
	once    sync.Once
}

func (n *gateNotifier) Notify(_ context.Context, event model.EscalationEvent) error {
	blocked := false
	if event.Level == model.LevelSupervisor {
		n.once.Do(func() { blocked = true })
	}
	if blocked {
		n.started <- event
		<-n.gate
	}
	return nil
}

// 一轮 Tick 中先进入投递的链阻塞在慢通道上，期间恢复另一条链：
// 后一条链的主管事件必须在投递前被抑制，不发给值班人员，链以 recovered 终止；
// 已被网关受理的先一条链通知如实记为 sent。
func TestRouter_RecoveryDuringTickSuppressesQueuedNotification(t *testing.T) {
	gate := &gateNotifier{
		gate:    make(chan struct{}),
		started: make(chan model.EscalationEvent, 1),
	}
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	now := base
	lines := []model.LineID{"line-1", "line-2"}
	router := NewRouter(DefaultRoster(lines), DefaultPolicy(), gate, func() time.Time { return now })

	a := model.NewAlarm("line-1", "A", "a", model.SeverityWarning, now)
	b := model.NewAlarm("line-2", "B", "b", model.SeverityWarning, now)
	router.HandleRaised(a)
	router.HandleRaised(b)
	chains := map[model.LineID]model.Alarm{"line-1": a, "line-2": b}

	now = now.Add(30 * time.Second)
	tickDone := make(chan struct{})
	go func() { router.Tick(context.Background()); close(tickDone) }()
	blockedEvent := <-gate.started

	// 恢复“另一条”链：它的主管事件已在阶段 1 生成，但尚未投递。
	otherID := model.LineID("line-2")
	if blockedEvent.LineID == otherID {
		otherID = "line-1"
	}
	other := chains[otherID]
	router.HandleRecovered(other.Recover(now))
	close(gate.gate)
	<-tickDone

	tlBlocked, _ := router.Timeline(chains[blockedEvent.LineID].ID)
	if last := tlBlocked.Events[len(tlBlocked.Events)-1]; last.Level != model.LevelSupervisor || last.Status != model.DeliverySent {
		t.Fatalf("accepted notification should be sent, got %s %s", last.Level, last.Status)
	}
	tlOther, _ := router.Timeline(other.ID)
	last := tlOther.Events[len(tlOther.Events)-1]
	if last.Level != model.LevelSupervisor || last.Status != model.DeliverySuppressed {
		t.Fatalf("queued notification must be suppressed before delivery, got line=%s level=%s status=%s",
			last.LineID, last.Level, last.Status)
	}
	if !tlOther.Terminated || tlOther.TerminateReason != model.ChainRecovered {
		t.Fatalf("recovered chain should terminate as recovered, got %+v", tlOther)
	}

	// 再等过下一个阈值：恢复的链不再升级经理；另一条正常升级。
	now = now.Add(30 * time.Second)
	router.Tick(context.Background())
	tlOther, _ = router.Timeline(other.ID)
	for _, e := range tlOther.Events {
		if e.Level == model.LevelManager {
			t.Fatalf("recovered chain must never notify manager")
		}
	}
	tlBlocked, _ = router.Timeline(chains[blockedEvent.LineID].ID)
	if last := tlBlocked.Events[len(tlBlocked.Events)-1]; last.Level != model.LevelManager || last.Status != model.DeliverySent {
		t.Fatalf("active chain should reach manager, got %+v", last)
	}
}

// 已到最高层终止后恢复信号才到达：终止原因应升级为 recovered，不被 top_level 粘住。
func TestRouter_RecoverReasonOverridesTopLevel(t *testing.T) {
	notifier := &recordNotifier{fail: map[string]bool{}}
	router, now := newTestRouter(t, notifier)

	a := warningAlarm(*now)
	router.HandleRaised(a)
	*now = now.Add(30 * time.Second)
	router.Tick(context.Background()) // supervisor
	*now = now.Add(30 * time.Second)
	router.Tick(context.Background()) // manager，到顶终止

	tl, _ := router.Timeline(a.ID)
	if !tl.Terminated || tl.TerminateReason != model.ChainTopLevel {
		t.Fatalf("precondition: top_level termination, got %+v", tl)
	}

	router.HandleRecovered(a.Recover(*now))
	tl, _ = router.Timeline(a.ID)
	if tl.TerminateReason != model.ChainRecovered || tl.Active {
		t.Fatalf("recover must override top_level reason, got reason=%s active=%v", tl.TerminateReason, tl.Active)
	}

	// 反向顺序不允许降级：确认不能覆盖恢复原因。
	router.HandleAcknowledged(a.Recover(*now).Acknowledge())
	tl, _ = router.Timeline(a.ID)
	if tl.TerminateReason != model.ChainRecovered {
		t.Fatalf("acknowledged must not downgrade recovered reason, got %s", tl.TerminateReason)
	}
}
