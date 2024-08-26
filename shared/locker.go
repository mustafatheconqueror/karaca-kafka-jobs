package shared

import "sync"

type MemoryLocker struct {
	mux sync.Mutex
}

func NewMemoryLocker() *MemoryLocker {
	return &MemoryLocker{}
}
func (s *MemoryLocker) Lock(key string) (success bool, err error) {
	s.mux.Lock()
	return true, nil
}

func (s *MemoryLocker) Unlock(key string) error {
	s.mux.Unlock()
	return nil
}
