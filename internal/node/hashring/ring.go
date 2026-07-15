// Package hashring provides consistent-hash ring for key-to-peer mapping.
//
// The hash ring maps blob hashes to edge nodes using CRC32-based consistent
// hashing with 150 virtual nodes per physical node. It is rebuilt from the
// local PeerEntryStore when peer membership changes, with a 30s buffer for
// newly-discovered peers and debounced rebuilds to avoid thrashing.
package hashring

import (
	"context"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shlande/mediaworker/internal/node/peerstore"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── consistentMap (internal ring implementation) ───

// consistentMap is a CRC32-based consistent hash ring that maps string keys
// to peer IDs. Each physical peer contributes replicas virtual nodes with
// distinct salts to ensure even distribution.
type consistentMap struct {
	replicas int
	keys     []uint32          // sorted CRC32 values of all virtual nodes
	hashMap  map[uint32]string // CRC32 → peer ID (string form)
}

func newConsistentMap(replicas int) *consistentMap {
	return &consistentMap{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
}

// add inserts replicas virtual nodes for the given peer into the map.
// buildKeys must be called after all peers are added to finalize the sorted key slice.
func (m *consistentMap) add(peer string) {
	for i := range m.replicas {
		h := crc32.ChecksumIEEE([]byte(peer + strconv.Itoa(i)))
		m.hashMap[h] = peer
	}
}

// buildKeys constructs the sorted key slice from hashMap entries.
func (m *consistentMap) buildKeys() {
	m.keys = make([]uint32, 0, len(m.hashMap))
	for k := range m.hashMap {
		m.keys = append(m.keys, k)
	}
	sort.Slice(m.keys, func(i, j int) bool { return m.keys[i] < m.keys[j] })
}

// get returns the peer ID responsible for the given key, or "" if the ring is empty.
func (m *consistentMap) get(key string) string {
	if len(m.hashMap) == 0 {
		return ""
	}
	h := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= h })
	if idx == len(m.keys) {
		idx = 0 // wrap around clockwise
	}
	return m.hashMap[m.keys[idx]]
}

// ─── HashRing ───

// HashRing is a consistent hash ring that maps blob hashes to edge node
// PeerIds. It is rebuilt from the local PeerEntryStore when peer membership
// changes, with a configurable new-peer buffer and debounced rebuild logic.
type HashRing struct {
	mu               sync.RWMutex
	ring             *consistentMap
	selfPeer         types.PeerId
	replicas         int
	entryStore       *peerstore.PeerEntryStore
	rebuildCh        chan struct{}
	rebuildCount     atomic.Int64
	peerJoinTime     sync.Map // map[types.PeerId]time.Time
	heartbeatTracker sync.Map // map[types.PeerId]int

	// Configurable durations (exposed for testing)
	newPeerBuf    time.Duration
	debounceWait  time.Duration
	maxWaitDur    time.Duration
	missThreshold int
}

// HashRingOption configures optional HashRing parameters (primarily for testing).
type HashRingOption func(*HashRing)

// WithNewPeerBuffer overrides the 30s new-peer exclusion window.
func WithNewPeerBuffer(d time.Duration) HashRingOption {
	return func(h *HashRing) { h.newPeerBuf = d }
}

// WithDebounce overrides the 1s debounce merge window.
func WithDebounce(d time.Duration) HashRingOption {
	return func(h *HashRing) { h.debounceWait = d }
}

// WithMaxWait overrides the 5s max-wait cap.
func WithMaxWait(d time.Duration) HashRingOption {
	return func(h *HashRing) { h.maxWaitDur = d }
}

// WithMissThreshold overrides the 3 consecutive heartbeat miss threshold.
func WithMissThreshold(n int) HashRingOption {
	return func(h *HashRing) { h.missThreshold = n }
}

// NewHashRing creates a HashRing with the given self peer, peer entry store,
// and virtual node count. The ring starts empty; call RebuildHashRing() to
// populate it.
func NewHashRing(self types.PeerId, store *peerstore.PeerEntryStore, replicas int, opts ...HashRingOption) *HashRing {
	h := &HashRing{
		selfPeer:      self,
		replicas:      replicas,
		entryStore:    store,
		rebuildCh:     make(chan struct{}, 256),
		newPeerBuf:    30 * time.Second,
		debounceWait:  time.Second,
		maxWaitDur:    5 * time.Second,
		missThreshold: 3,
	}
	for _, o := range opts {
		o(h)
	}
	h.ring = newConsistentMap(replicas)
	return h
}

// Get returns the primary node PeerId for the given blob hash.
func (h *HashRing) Get(blobHash string) types.PeerId {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return types.PeerId(h.ring.get(blobHash))
}

// IsPrimary returns true if self is the primary node for the given blob hash.
func (h *HashRing) IsPrimary(blobHash string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ring.get(blobHash) == string(h.selfPeer)
}

// Replace atomically swaps the underlying consistent hash ring.
func (h *HashRing) Replace(newRing *consistentMap) {
	h.mu.Lock()
	h.ring = newRing
	h.mu.Unlock()
}

// RebuildHashRing rebuilds the ring from PeerEntryStore.ActivePeers().
// Only peers with PeerICP=true capability are included. Peers whose join
// time is within the new-peer buffer window are excluded to prevent
// transient DHT-discovered peers from destabilizing routing.
func (h *HashRing) RebuildHashRing() {
	defer h.rebuildCount.Add(1)

	peers := h.entryStore.ActivePeers()
	now := time.Now()

	newRing := newConsistentMap(h.replicas)
	for _, p := range peers {
		if !p.Capabilities.PeerICP {
			continue
		}
		// 30s new-peer buffer: exclude peers that joined too recently
		if jt, ok := h.peerJoinTime.Load(p.PeerID); ok {
			if joinTime, ok2 := jt.(time.Time); ok2 {
				if now.Sub(joinTime) < h.newPeerBuf {
					continue
				}
			}
		}
		newRing.add(string(p.PeerID))
	}
	newRing.buildKeys()

	h.Replace(newRing)
}

// RebuildCount returns the number of times RebuildHashRing has been called.
// Primarily for testing.
func (h *HashRing) RebuildCount() int64 {
	return h.rebuildCount.Load()
}

// OnPeerStoreChange triggers a debounced ring rebuild. If rebuilds are
// already queued with an active debounce timer, this call extends the
// debounce window (up to the max-wait cap).
func (h *HashRing) OnPeerStoreChange() {
	select {
	case h.rebuildCh <- struct{}{}:
	default:
		// Channel full; a rebuild is already queued.
	}
}

// RecordPeerJoin records the join time for the given peer. The peer is
// excluded from the ring for newPeerBuf duration after this call.
func (h *HashRing) RecordPeerJoin(peerID types.PeerId) {
	h.peerJoinTime.Store(peerID, time.Now())
}

// OnHeartbeat resets the missed heartbeat counter for the given peer.
func (h *HashRing) OnHeartbeat(peerID types.PeerId) {
	h.heartbeatTracker.Store(peerID, 0)
}

// OnHeartbeatMiss increments the missed heartbeat counter for the given
// peer. When the counter reaches the miss threshold (default 3), the peer
// is marked stale in the PeerEntryStore.
func (h *HashRing) OnHeartbeatMiss(peerID types.PeerId) {
	val, _ := h.heartbeatTracker.LoadOrStore(peerID, 0)
	count := val.(int) + 1
	h.heartbeatTracker.Store(peerID, count)
	if count >= h.missThreshold {
		h.entryStore.MarkStale(peerID)
	}
}

// StartRebuildLoop runs a goroutine that listens on the rebuild channel
// with debounce and max-wait logic. On each rebuild trigger, it calls
// RebuildHashRing. The goroutine exits when ctx is cancelled.
func (h *HashRing) StartRebuildLoop(ctx context.Context) {
	go func() {
		var timer *time.Timer
		var firstEvent time.Time
		timerActive := false

		for {
			var timerCh <-chan time.Time
			if timerActive {
				timerCh = timer.C
			}

			select {
			case <-ctx.Done():
				if timerActive {
					timer.Stop()
				}
				return
			case <-timerCh:
				h.RebuildHashRing()
				timerActive = false
			case <-h.rebuildCh:
				if !timerActive {
					firstEvent = time.Now()
					timer = time.NewTimer(h.debounceWait)
					timerActive = true
				} else {
					if time.Since(firstEvent) >= h.maxWaitDur {
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
						h.RebuildHashRing()
						timerActive = false
						continue
					}
					// Reset debounce timer (extend merge window)
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(h.debounceWait)
				}
			}
		}
	}()
}
