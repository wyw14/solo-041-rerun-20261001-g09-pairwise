package alarm

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

// Hook receives alarm lifecycle changes. Callbacks run outside the service
// lock, so hooks must not call back into the same Service.
type Hook interface {
	AlarmRaised(model.Alarm)
	AlarmAcknowledged(model.Alarm)
	AlarmRecovered(model.Alarm)
}

type Service struct {
	mu     sync.RWMutex
	alarms map[uuid.UUID]model.Alarm
	hooks  []Hook
}

func NewService() *Service {
	return &Service{alarms: make(map[uuid.UUID]model.Alarm)}
}

// Subscribe registers a lifecycle hook. It must be called before any alarm
// is raised; late subscribers do not receive past alarms.
func (s *Service) Subscribe(hook Hook) {
	if hook == nil {
		return
	}
	s.mu.Lock()
	s.hooks = append(s.hooks, hook)
	s.mu.Unlock()
}

func (s *Service) dispatch(handle func(Hook)) {
	s.mu.RLock()
	hooks := make([]Hook, len(s.hooks))
	copy(hooks, s.hooks)
	s.mu.RUnlock()
	for _, hook := range hooks {
		handle(hook)
	}
}

func (s *Service) Raise(alarm model.Alarm) model.Alarm {
	s.mu.Lock()
	s.alarms[alarm.ID] = alarm
	s.mu.Unlock()
	s.dispatch(func(h Hook) { h.AlarmRaised(alarm) })
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
	s.dispatch(func(h Hook) { h.AlarmAcknowledged(value) })
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
	s.dispatch(func(h Hook) { h.AlarmRecovered(value) })
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
