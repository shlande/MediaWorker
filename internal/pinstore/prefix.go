package pinstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// PrefixPartition manages blob data on an NVMe prefix partition.
type PrefixPartition struct {
	rootPath string
	maxSize  int64
	usedSize atomic.Int64
}

func newPrefixPartition(rootPath string, maxSize int64) *PrefixPartition {
	return &PrefixPartition{rootPath: rootPath, maxSize: maxSize}
}

func (pp *PrefixPartition) blobPath(blobHash string) string {
	return filepath.Join(pp.rootPath, blobHash)
}

func (pp *PrefixPartition) Put(blobHash string, data []byte) error {
	path := pp.blobPath(blobHash)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("pinstore prefix put %s: %w", blobHash, err)
	}
	pp.usedSize.Add(int64(len(data)))
	return nil
}

func (pp *PrefixPartition) Get(blobHash string) ([]byte, bool) {
	data, err := os.ReadFile(pp.blobPath(blobHash))
	if err != nil {
		return nil, false
	}
	return data, true
}

func (pp *PrefixPartition) Has(blobHash string) bool {
	_, err := os.Stat(pp.blobPath(blobHash))
	return err == nil
}

func (pp *PrefixPartition) RemoveContent(blobHash string) error {
	path := pp.blobPath(blobHash)
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("pinstore prefix remove stat %s: %w", blobHash, err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("pinstore prefix remove %s: %w", blobHash, err)
	}
	pp.usedSize.Add(-st.Size())
	return nil
}

func (pp *PrefixPartition) Available() int64 {
	return pp.maxSize - pp.usedSize.Load()
}
