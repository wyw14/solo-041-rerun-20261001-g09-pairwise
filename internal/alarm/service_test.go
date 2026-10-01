package alarm

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/notify"
)

// 端到端：alarm.Service 的 Raise/Acknowledge/Recover 通过钩子驱动通知路由器，
// 并在 Tick 中按持续时间升级，恢复后终止。
func TestService_HooksDriveEscalationRouter(t *testing.T) {
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	now := base
	service := NewService()
	router := notify.NewRouter(
		notify.DefaultRoster([]model.LineID{"line-1"}),
		notify.DefaultPolicy(),
		notify.NewLogNotifier(nil),
		func() time.Time { return now },
	)
	service.Subscribe(func(a model.Alarm) {
		switch {
		case !a.Active():
			router.HandleRecovered(a)
		case a.Acknowledged:
			router.HandleAcknowledged(a)
		default:
			router.HandleRaised(a)
		}
	})

	a := service.Raise(model.NewAlarm("line-1", "X", "x", model.SeverityWarning, now))
	tl, err := router.Timeline(a.ID)
	if err != nil || len(tl.Events) != 1 || tl.CurrentLevel != model.LevelOperator {
		t.Fatalf("raise hook should create operator event, err=%v tl=%+v", err, tl)
	}

	now = now.Add(30 * time.Second)
	router.Tick(context.Background())
	tl, _ = router.Timeline(a.ID)
	if tl.CurrentLevel != model.LevelSupervisor || len(tl.Events) != 2 {
		t.Fatalf("after 30s unacknowledged, expect supervisor: %+v", tl)
	}

	// 通过服务恢复（模拟报警恢复），升级链应终止。
	if _, err := service.Recover(a.ID, now); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Minute)
	router.Tick(context.Background())
	tl, _ = router.Timeline(a.ID)
	if !tl.Terminated || tl.TerminateReason != model.ChainRecovered {
		t.Fatalf("recover hook should terminate chain: %+v", tl)
	}
	if len(tl.Events) != 2 {
		t.Fatalf("history of 2 notifications should remain, got %d", len(tl.Events))
	}
}
