package api

import (
	"encoding/json"
	"net/http"

	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/repo"
	"github.com/bond-kaneko/fill-job-poc/custom-impl/internal/service"
	"github.com/bond-kaneko/fill-job-poc/shared/task"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	svc  *service.Service
	pool *pgxpool.Pool
}

func New(svc *service.Service, pool *pgxpool.Pool) *Handler {
	return &Handler{svc: svc, pool: pool}
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", h.createTask)
	mux.HandleFunc("GET /tasks/{id}", h.getTask)
	return mux
}

type createReq struct {
	TaskID  string       `json:"task_id,omitempty"`
	Payload task.Payload `json:"payload"`
}

type createResp struct {
	TaskID string `json:"task_id"`
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.TaskID == "" {
		req.TaskID = uuid.NewString()
	}
	if _, err := h.svc.CreateTask(r.Context(), req.TaskID, req.Payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, createResp{TaskID: req.TaskID})
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := repo.LoadTask(r.Context(), h.pool, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
