package failover

import (
	"context"
)

func (policy *Policy) RequestContext() context.Context {
	if policy.WriterContext != nil {
		return policy.WriterContext
	}
	return context.Background()
}
func (policy *Policy) StopWriter() {
	policy.WriterMu.Lock()
	policy.Stopped.Store(true)
	if policy.CancelWriter != nil {
		policy.CancelWriter()
	}
	policy.WriterMu.Unlock()
	policy.WriterOperations.Wait()
}
func (policy *Policy) BeginWriterOperation() bool {
	policy.WriterMu.Lock()
	defer policy.WriterMu.Unlock()
	if policy.Stopped.Load() {
		return false
	}
	policy.WriterOperations.Add(1)
	return true
}
