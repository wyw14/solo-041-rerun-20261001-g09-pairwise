package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func (s *Server) alarmList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"alarms": s.plant.Alarms()})
}

func (s *Server) acknowledgeAlarm(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "alarmID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	alarm, err := s.plant.AcknowledgeAlarm(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, alarm)
}

func (s *Server) recoverAlarm(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "alarmID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	alarm, err := s.plant.RecoverAlarm(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, alarm)
}

// escalationTimeline 返回单条报警的升级时间线：每次通知的触发依据、
// 投递对象与投递状态，以及升级链是否已终止。
func (s *Server) escalationTimeline(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "alarmID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	timeline, err := s.plant.EscalationTimeline(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, timeline)
}

// escalationTimelines 返回全部升级时间线，可用 ?line_id= 过滤处理线。
func (s *Server) escalationTimelines(w http.ResponseWriter, r *http.Request) {
	lineID, _ := model.ParseLineID(r.URL.Query().Get("line_id"))
	writeJSON(w, http.StatusOK, map[string]any{
		"timelines": s.plant.EscalationTimelines(lineID),
	})
}
