package worker

// FillTaskArgs は River の job 引数。
// Kind() で job の種別を返す (River の規約)。
type FillTaskArgs struct {
	TaskID string `json:"task_id"`
}

func (FillTaskArgs) Kind() string { return "fill_task" }
