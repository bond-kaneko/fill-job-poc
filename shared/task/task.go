package task

import (
	"context"
	"errors"
	"strings"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type Task struct {
	ID        string
	Payload   Payload
	Status    Status
	Result    *Result
	FillError *string
	FilledAt  *time.Time
	CreatedAt time.Time
}

type Payload struct {
	Echo string `json:"echo"`
	// ComputeMs は通常の擬似処理時間。
	ComputeMs int `json:"compute_ms,omitempty"`
	// FailUntilAttempt は attempt < FailUntilAttempt の試行で transient エラーを返す。
	// 例: FailUntilAttempt=3 なら attempt 1,2 で fail、attempt 3 で成功。
	FailUntilAttempt int `json:"fail_until_attempt,omitempty"`
	// AlwaysFail は恒久エラーを返す。retry しても無駄。
	AlwaysFail bool `json:"always_fail,omitempty"`
	// HangFirstAttempts は最初の N 回の attempt で長時間 (60s) hang する。
	// crash / timeout 復旧テスト用。worker の lease/timeout より長い時間。
	HangFirstAttempts int `json:"hang_first_attempts,omitempty"`
}

type Result struct {
	Echo string `json:"echo"`
}

var (
	ErrInvalidTransition = errors.New("invalid status transition")
)

// Process は payload に従って worker が実行する擬似的な「重い処理」。
// attempt は 1-indexed (1 回目の試行で 1)。
//
// 戻り値: (Result, transient error, permanent error)
//   - Result が non-nil: 成功
//   - transient error: retry させたい一時障害
//   - permanent error: retry しても無駄な恒久エラー
//
// ctx が cancel された場合は (nil, ctx.Err(), nil) を返す (= transient 扱い)。
func Process(ctx context.Context, p Payload, attempt int) (*Result, error, error) {
	if attempt <= p.HangFirstAttempts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err(), nil
		case <-time.After(60 * time.Second):
		}
	}
	if p.ComputeMs > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err(), nil
		case <-time.After(time.Duration(p.ComputeMs) * time.Millisecond):
		}
	}
	if p.AlwaysFail {
		return nil, nil, errors.New("payload.always_fail=true")
	}
	if attempt < p.FailUntilAttempt {
		return nil, errors.New("transient: attempt below fail_until_attempt"), nil
	}
	return &Result{Echo: strings.ToUpper(p.Echo)}, nil, nil
}
