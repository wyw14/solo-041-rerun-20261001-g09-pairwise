package notify

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

// chainState 是一条报警的升级链内部状态。
type chainState struct {
	alarm      model.Alarm
	events     []model.EscalationEvent
	levelIdx   int // 已通知到的最高层级在 Policy.Chain 中的下标
	terminated bool
	termReason string
	termAt     *time.Time
}

type pendingDelivery struct {
	chain *chainState
	event model.EscalationEvent
}

// Router 是分级通知路由器：依据报警的严重度、处理线、持续时间和确认状态，
// 把未处理报警逐级升级给下一层值班人员。
//
// 路由输入来自报警服务的三个钩子（Raise/Acknowledge/Recover）和周期性 Tick。
// 通知事件的生成与去重占位在同一把锁内原子完成，之后才在锁外执行实际投递，
// 因此重复 Tick 或并发钩子都不会刷出重复通知。
type Router struct {
	mu       sync.Mutex
	roster   *Roster
	policy   Policy
	notifier Notifier
	now      func() time.Time
	chains   map[uuid.UUID]*chainState
}

func NewRouter(roster *Roster, policy Policy, notifier Notifier, now func() time.Time) *Router {
	if roster == nil {
		roster = NewRoster()
	}
	if notifier == nil {
		notifier = NewLogNotifier(nil)
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Router{
		roster:   roster,
		policy:   policy,
		notifier: notifier,
		now:      now,
		chains:   make(map[uuid.UUID]*chainState),
	}
}

// HandleRaised 处理新报警：按严重度选择起始层级并立即投递首条通知。
// 危急报警跳过一线值班员，直接从值班主管开始。同一报警重复进入是幂等的。
func (r *Router) HandleRaised(alarm model.Alarm) {
	start, ok := r.policy.StartLevel[alarm.Severity]
	startIdx := indexOf(r.policy.Chain, start)
	if !ok || startIdx < 0 {
		start = r.policy.Chain[0]
		startIdx = 0
	}
	now := r.now().UTC()
	event := r.buildEvent(alarm, start, model.ReasonRaised, model.EscalationBasis{
		TriggeredAt: now,
		RaisedAt:    alarm.RaisedAt,
		Rule:        "raise:severity=" + string(alarm.Severity) + " routes to " + string(start),
	}, now)
	event.Status = model.DeliveryPending

	r.mu.Lock()
	if _, exists := r.chains[alarm.ID]; exists {
		r.mu.Unlock()
		return
	}
	chain := &chainState{
		alarm:    alarm,
		levelIdx: startIdx,
		events:   []model.EscalationEvent{event},
	}
	r.chains[alarm.ID] = chain
	r.mu.Unlock()

	r.deliver(context.Background(), pendingDelivery{chain: chain, event: event})
}

// HandleAcknowledged 在报警被确认时终止后续升级：已确认的报警不再上行。
// 已经发出的通知作为历史保留在时间线中。
func (r *Router) HandleAcknowledged(alarm model.Alarm) {
	r.mu.Lock()
	defer r.mu.Unlock()
	chain, ok := r.chains[alarm.ID]
	if !ok {
		return
	}
	chain.alarm = alarm
	r.terminateLocked(chain, model.ChainAcknowledged)
}

// HandleRecovered 在报警恢复时终止后续升级。
func (r *Router) HandleRecovered(alarm model.Alarm) {
	r.mu.Lock()
	defer r.mu.Unlock()
	chain, ok := r.chains[alarm.ID]
	if !ok {
		return
	}
	chain.alarm = alarm
	r.terminateLocked(chain, model.ChainRecovered)
}

// Tick 扫描所有未终止的升级链，把持续未确认且达到等待阈值的报警
// 升级给下一层值班人员。重复调用是幂等的：同一层级不会产生第二条通知。
// 返回本次实际新产生的通知事件。
func (r *Router) Tick(ctx context.Context) []model.EscalationEvent {
	now := r.now().UTC()

	// 第一阶段（锁内）：判定到期、生成事件并占位去重。
	r.mu.Lock()
	pending := make([]pendingDelivery, 0)
	for _, chain := range r.chains {
		if chain.terminated {
			continue
		}
		switch {
		case chain.alarm.Acknowledged:
			r.terminateLocked(chain, model.ChainAcknowledged)
			continue
		case !chain.alarm.Active():
			r.terminateLocked(chain, model.ChainRecovered)
			continue
		}
		nextIdx := chain.levelIdx + 1
		if nextIdx >= len(r.policy.Chain) {
			r.terminateLocked(chain, model.ChainTopLevel)
			continue
		}
		if len(chain.events) == 0 {
			continue
		}
		threshold := r.waitFor(chain.alarm.Severity, chain.levelIdx)
		if threshold <= 0 {
			continue
		}
		last := chain.events[len(chain.events)-1]
		if now.Before(last.NotifiedAt.Add(threshold)) {
			continue
		}
		currentLevel := r.policy.Chain[chain.levelIdx]
		nextLevel := r.policy.Chain[nextIdx]
		event := r.buildEvent(chain.alarm, nextLevel, model.ReasonTimeout, model.EscalationBasis{
			TriggeredAt:         now,
			RaisedAt:            chain.alarm.RaisedAt,
			DurationMillis:      now.Sub(chain.alarm.RaisedAt).Milliseconds(),
			Acknowledged:        false,
			PreviousLevel:       currentLevel,
			WaitThresholdMillis: threshold.Milliseconds(),
			Rule: "timeout:unacknowledged for " + threshold.String() +
				" past " + string(currentLevel) + ", escalate to " + string(nextLevel),
		}, now)
		event.Status = model.DeliveryPending
		// 占位即去重：层级下标前移与事件落账在同一把锁内完成，
		// 之后任何重复扫描看到的都是更高层级，不会再为该层级生成通知。
		chain.levelIdx = nextIdx
		chain.events = append(chain.events, event)
		pending = append(pending, pendingDelivery{chain: chain, event: event})
	}
	r.mu.Unlock()

	// 第二阶段（锁外）：执行实际投递，避免短信/呼叫等慢通道阻塞报警钩子。
	created := make([]model.EscalationEvent, 0, len(pending))
	for _, item := range pending {
		created = append(created, r.deliver(ctx, item))
	}
	return created
}

// Timeline 返回一条报警的完整升级时间线。
func (r *Router) Timeline(alarmID uuid.UUID) (model.EscalationTimeline, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	chain, ok := r.chains[alarmID]
	if !ok {
		return model.EscalationTimeline{}, model.ErrEscalationNotFound
	}
	return r.snapshotLocked(chain), nil
}

// Timelines 返回升级时间线，可按处理线过滤（传空表示全部）。
// 仍活跃的链排在前面，组内按最近一次通知时间倒序。
func (r *Router) Timelines(lineID model.LineID) []model.EscalationTimeline {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]model.EscalationTimeline, 0)
	for _, chain := range r.chains {
		if lineID != "" && chain.alarm.LineID != lineID {
			continue
		}
		result = append(result, r.snapshotLocked(chain))
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Terminated != result[j].Terminated {
			return !result[i].Terminated // 未终止在前
		}
		return latestNotify(result[i].Events).After(latestNotify(result[j].Events))
	})
	return result
}

func latestNotify(events []model.EscalationEvent) time.Time {
	if len(events) == 0 {
		return time.Time{}
	}
	return events[len(events)-1].NotifiedAt
}

func (r *Router) snapshotLocked(chain *chainState) model.EscalationTimeline {
	events := make([]model.EscalationEvent, len(chain.events))
	copy(events, chain.events)
	return model.EscalationTimeline{
		AlarmID:         chain.alarm.ID,
		LineID:          chain.alarm.LineID,
		Severity:        chain.alarm.Severity,
		CurrentLevel:    r.policy.Chain[chain.levelIdx],
		Acknowledged:    chain.alarm.Acknowledged,
		Active:          chain.alarm.Active(),
		Terminated:      chain.terminated,
		TerminateReason: chain.termReason,
		TerminatedAt:    chain.termAt,
		Events:          events,
	}
}

func (r *Router) buildEvent(alarm model.Alarm, level model.DutyLevel, reason model.EscalationReason,
	basis model.EscalationBasis, now time.Time) model.EscalationEvent {
	if basis.TriggeredAt.IsZero() {
		basis.TriggeredAt = now
	}
	return model.EscalationEvent{
		ID:         uuid.New(),
		AlarmID:    alarm.ID,
		LineID:     alarm.LineID,
		Severity:   alarm.Severity,
		Level:      level,
		Reason:     reason,
		Basis:      basis,
		Recipients: r.roster.Contacts(alarm.LineID, level),
		NotifiedAt: now,
	}
}

// deliver 在锁外执行投递，再回到锁内按事件 ID 回写投递状态。
// 生成通知后、实际投递前，报警可能已被确认或恢复（慢通道场景）：
// 此时不再向值班人员发出该通知，改记为 suppressed，事件与触发依据仍保留。
func (r *Router) deliver(ctx context.Context, item pendingDelivery) model.EscalationEvent {
	event := item.event

	// 预检：事件生成后、调用投递通道前，若报警已被确认/恢复（典型场景：
	// 本轮 Tick 中排在一条慢通道之后才轮到本链），则不再通知值班人员，
	// 记为 suppressed，事件与触发依据仍留在时间线。
	r.mu.Lock()
	chain := item.chain
	if chain.terminated || !chain.alarm.Active() || chain.alarm.Acknowledged {
		r.mu.Unlock()
		return r.finalize(chain, event, model.DeliverySuppressed, "")
	}
	r.mu.Unlock()

	// 锁外执行实际投递（短信/呼叫等慢通道）。投递往返期间报警可能恰好恢复：
	// 网关既已受理，状态如实记为 sent，但升级链仍按 recovered/acknowledged
	// 终止，且不会再产生任何后续层级的通知。
	status, errText := model.DeliverySent, ""
	if err := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return r.notifier.Notify(ctx, event)
	}(); err != nil {
		status, errText = model.DeliveryFailed, err.Error()
	}
	return r.finalize(chain, event, status, errText)
}

// finalize 回到锁内按事件 ID 回写投递状态；到顶且成功送达时终止升级链。
func (r *Router) finalize(chain *chainState, event model.EscalationEvent,
	status model.DeliveryStatus, errText string) model.EscalationEvent {
	event.Status = status
	event.DeliveryErr = errText

	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range chain.events {
		if chain.events[i].ID == event.ID {
			chain.events[i] = event
			break
		}
	}
	if status == model.DeliverySent && chain.levelIdx >= len(r.policy.Chain)-1 &&
		!chain.terminated && chain.alarm.Active() && !chain.alarm.Acknowledged {
		r.terminateLocked(chain, model.ChainTopLevel)
	}
	return event
}

// waitFor 取“从当前层级跃迁到下一层”的等待阈值。
// 超时序列按该严重度起始层级之后的跃迁顺序排列。
func (r *Router) waitFor(severity model.AlarmSeverity, currentLevelIdx int) time.Duration {
	timeouts := r.policy.TimeoutsFor(severity)
	start, ok := r.policy.StartLevel[severity]
	startIdx := indexOf(r.policy.Chain, start)
	if !ok || startIdx < 0 {
		startIdx = 0
	}
	step := currentLevelIdx - startIdx
	if step < 0 || step >= len(timeouts) {
		return 0
	}
	return timeouts[step]
}

// terminateReasonRank 定义终止原因的优先级：报警物理恢复最高，确认次之，
// 到顶只是“通知完了”。这样即便到顶终止后恢复信号才到达，时间线记录的
// 终止原因仍是真实的 recovered/acknowledged。
var terminateReasonRank = map[string]int{
	model.ChainTopLevel:     1,
	model.ChainAcknowledged: 2,
	model.ChainRecovered:    3,
}

func (r *Router) terminateLocked(chain *chainState, reason string) {
	now := r.now().UTC()
	if !chain.terminated {
		chain.terminated = true
		chain.termReason = reason
		chain.termAt = &now
		return
	}
	// 已终止时允许更高优先级的原因覆盖（如恢复信号晚于到顶终止到达），
	// 终止时间保留最早的一次。
	if terminateReasonRank[reason] > terminateReasonRank[chain.termReason] {
		chain.termReason = reason
	}
}

func indexOf(chain []model.DutyLevel, level model.DutyLevel) int {
	for i, candidate := range chain {
		if candidate == level {
			return i
		}
	}
	return -1
}
