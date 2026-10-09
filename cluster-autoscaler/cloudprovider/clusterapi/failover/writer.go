package failover

import (
	"context"
)

// RequestContext returns the writer's cancellable context or a background default.
func (policy *Policy) RequestContext() context.Context {
	if policy.WriterContext != nil {
		return policy.WriterContext
	}
	return context.Background()
}
// StopWriter closes local admission, cancels requests and waits for tracked work.
// It does not establish distributed fencing or determine remote commit outcomes.
func (policy *Policy) StopWriter() {
	policy.WriterMu.Lock()
	policy.Stopped.Store(true)
	if policy.CancelWriter != nil {
		policy.CancelWriter()
	}
	policy.WriterMu.Unlock()
	policy.WriterOperations.Wait()
}
// BeginWriterOperation admits work that the caller must finish with WriterOperations.Done.
func (policy *Policy) BeginWriterOperation() bool {
	policy.WriterMu.Lock()
	defer policy.WriterMu.Unlock()
	if policy.Stopped.Load() {
		return false
	}
	policy.WriterOperations.Add(1)
	return true
}
