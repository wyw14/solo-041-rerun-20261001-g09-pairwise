package alarm

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

// Hook 在报警产生、被确认或恢复时被回调。钩子在服务锁外执行，
// 不得阻塞；典型实现是把事件投递给通知升级路由器。
type Hook func(alarm model.Alarm)

type Service struct {
	mu     sync.RWMutex
	alarms map[uuid.UUID]model.Alarm
	hooks  []Hook
}

func NewService() *Service {
	return &Service{alarms: make(map[uuid.UUID]model.Alarm)}
}

// Subscribe 注册报警变更钩子（产生/确认/恢复）。必须在开始产生报警前注册。
func (s *Service) Subscribe(hook Hook) {
	if hook == nil {
		return
	}
	s.mu.Lock()
	s.hooks = append(s.hooks, hook)
	s.mu.Unlock()
}

func (s *Service) emit(alarm model.Alarm) {
	s.mu.RLock()
	hooks := make([]Hook, len(s.hooks))
	copy(hooks, s.hooks)
	s.mu.RUnlock()
	for _, hook := range hooks {
		hook(alarm)
	}
}

func (s *Service) Raise(alarm model.Alarm) model.Alarm {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
	s.emit(alarm)
	return alarm
}

func (s *Service) Acknowledge(id uuid.UUID) (model.Alarm, error) {
	s.mu.Lock()
	value, ok := s.alarms[id]
	if !ok {
		s.mu.Unlock()
		return model.Alarm{}, errors.New("alarm is not found")
	}
	value = value.Acknowledge()
	s.alarms[id] = value
	s.mu.Unlock()
	s.emit(value)
	return value, nil
}

func (s *Service) Recover(id uuid.UUID, now time.Time) (model.Alarm, error) {
	s.mu.Lock()
	value, ok := s.alarms[id]
	if !ok {
		s.mu.Unlock()
		return model.Alarm{}, errors.New("alarm is not found")
	}
	value = value.Recover(now)
	s.alarms[id] = value
	s.mu.Unlock()
	s.emit(value)
	return value, nil
}

func (s *Service) Active(lineID model.LineID) []model.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Alarm, 0)
	for _, value := range s.alarms {
		if value.LineID == lineID && value.Active() {
			result = append(result, value)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RaisedAt.Before(result[j].RaisedAt) })
	return result
}

func (s *Service) All() []model.Alarm {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Alarm, 0, len(s.alarms))
	for _, value := range s.alarms {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RaisedAt.After(result[j].RaisedAt) })
	return result
}
