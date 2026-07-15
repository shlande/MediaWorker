package routing

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/shlande/mediaworker/internal/types"
)

// Scheduler simulates DNS+302 scheduling for edge node selection.
// It maintains a set of configured edge nodes and routes requests
// to the first healthy node in the requested region.
type Scheduler struct {
	mu    sync.RWMutex
	nodes []NodeEndpoint
}

// NodeEndpoint describes a configured edge node known to the scheduler.
type NodeEndpoint struct {
	PeerID  types.PeerId
	Addr    string // "host:port"
	Region  string // "beijing", "guangzhou", "singapore"
	Healthy bool
}

// NewScheduler creates a Scheduler with the given edge node endpoints.
func NewScheduler(nodes []NodeEndpoint) *Scheduler {
	return &Scheduler{nodes: nodes}
}

// ResolveDNS returns the address of the first healthy node in the given region.
// Returns an error if no healthy node is found.
func (s *Scheduler) ResolveDNS(region string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := range s.nodes {
		n := &s.nodes[i]
		if n.Region == region && n.Healthy {
			return n.Addr, nil
		}
	}
	return "", fmt.Errorf("no healthy node in region %q", region)
}

// HandleHTTP302 writes a 302 redirect to the next healthy backup node.
// If the current primary (identified by addr) is healthy, it does nothing
// (the caller should have already served the response). If the primary is
// unhealthy, it redirects to the first healthy backup in any region.
func (s *Scheduler) HandleHTTP302(w http.ResponseWriter, r *http.Request, primaryAddr string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if primary is healthy.
	for i := range s.nodes {
		if s.nodes[i].Addr == primaryAddr && s.nodes[i].Healthy {
			// Primary is healthy — nothing to redirect.
			return
		}
	}

	// Primary unhealthy — find first healthy backup.
	for i := range s.nodes {
		if s.nodes[i].Healthy && s.nodes[i].Addr != primaryAddr {
			http.Redirect(w, r, "http://"+s.nodes[i].Addr+r.URL.RequestURI(), http.StatusFound)
			return
		}
	}

	// No healthy node available at all.
	http.Error(w, "no healthy edge node available", http.StatusServiceUnavailable)
}

// MarkUnhealthy sets the node identified by peerID as unhealthy.
func (s *Scheduler) MarkUnhealthy(peerID types.PeerId) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.nodes {
		if s.nodes[i].PeerID == peerID {
			s.nodes[i].Healthy = false
			return
		}
	}
}

// MarkHealthy sets the node identified by peerID as healthy.
func (s *Scheduler) MarkHealthy(peerID types.PeerId) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.nodes {
		if s.nodes[i].PeerID == peerID {
			s.nodes[i].Healthy = true
			return
		}
	}
}

// HealthyNodes returns the count of currently healthy nodes.
func (s *Scheduler) HealthyNodes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for i := range s.nodes {
		if s.nodes[i].Healthy {
			count++
		}
	}
	return count
}
