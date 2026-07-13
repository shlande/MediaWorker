package jwt

import (
	"sync"

	"github.com/shlande/mediaworker/internal/types"
)

// PeerIdSet is a thread-safe set of PeerIds used for the L4 whitelist.
// L4 nodes receive special credentials that entitle them to backhaul storage, so
// the whitelist is manually managed and read-heavy (checked on every JWT request).
type PeerIdSet struct {
	mu sync.RWMutex
	m  map[types.PeerId]bool
}

// NewPeerIdSet returns an empty PeerIdSet.
func NewPeerIdSet() *PeerIdSet {
	return &PeerIdSet{
		m: make(map[types.PeerId]bool),
	}
}

// Contains reports whether p is in the set.
func (s *PeerIdSet) Contains(p types.PeerId) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[p]
}

// Add inserts p into the set.
func (s *PeerIdSet) Add(p types.PeerId) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[p] = true
}

// Remove deletes p from the set.
func (s *PeerIdSet) Remove(p types.PeerId) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, p)
}
