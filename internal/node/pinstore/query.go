package pinstore

// Pin lifecycle states for PinEntry.State. The wire/API contract
// (docs/ui-api-requirements.md E6) is exactly these three values — no
// finer-grained progress states by design.
const (
	PinStateReady   = "ready"
	PinStatePulling = "pulling"
	PinStateFailed  = "failed"
)

// PinFilter narrows List results. The zero value matches every pin.
type PinFilter struct {
	// Ready, when non-nil, matches on (State == PinStateReady) == *Ready;
	// Ready=false therefore matches both pulling and failed pins.
	Ready     *bool
	Role      string // "" = any role
	ContentID string // "" = any content
}

// List returns a snapshot of all pinned entries matching filter.
func (ps *PinStore) List(filter PinFilter) []PinEntry {
	out := make([]PinEntry, 0, ps.pinCount())
	ps.stateMu.RLock()
	defer ps.stateMu.RUnlock()
	ps.index.Range(func(_, val any) bool {
		entry, ok := val.(*PinEntry)
		if !ok {
			return true
		}
		if filter.Ready != nil && (entry.State == PinStateReady) != *filter.Ready {
			return true
		}
		if filter.Role != "" && entry.Role != filter.Role {
			return true
		}
		if filter.ContentID != "" && entry.ContentID != filter.ContentID {
			return true
		}
		out = append(out, snapshotPinEntry(entry))
		return true
	})
	return out
}

// Get returns a snapshot of the pin entry for blobHash.
func (ps *PinStore) Get(blobHash string) (PinEntry, bool) {
	val, ok := ps.index.Load(blobHash)
	if !ok {
		return PinEntry{}, false
	}
	entry, ok := val.(*PinEntry)
	if !ok {
		return PinEntry{}, false
	}
	ps.stateMu.RLock()
	defer ps.stateMu.RUnlock()
	return snapshotPinEntry(entry), true
}

// RetryPin re-triggers the fetch for a failed pin: the entry resets to
// pulling with LastError cleared and fetchPinnedBlob runs again (same
// semantics as the ApplyPin fetch). Returns false when the pin does not
// exist or is not in State=failed. Idempotent: a second call while the
// retry is in flight observes State=pulling and returns false.
func (ps *PinStore) RetryPin(blobHash string) bool {
	if !ps.setPinState(blobHash, PinStateFailed, PinStatePulling, "") {
		return false
	}
	go ps.fetchPinnedBlob(blobHash)
	return true
}

// snapshotPinEntry copies an entry's scalar fields. Callers must hold
// stateMu (read or write). Ready is re-Stored rather than copied so the
// snapshot never aliases the live atomic. Named result + naked return:
// copylocks-clean construction of a fresh atomic.Bool.
func snapshotPinEntry(e *PinEntry) (ne PinEntry) {
	ne = PinEntry{
		BlobHash:  e.BlobHash,
		BlobType:  e.BlobType,
		Role:      e.Role,
		Size:      e.Size,
		PinnedAt:  e.PinnedAt,
		ContentID: e.ContentID,
		State:     e.State,
		LastError: e.LastError,
	}
	ne.Ready.Store(e.Ready.Load())
	return
}
