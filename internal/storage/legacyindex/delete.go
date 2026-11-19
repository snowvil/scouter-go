// Copyright 2015 the original author or authors (Scouter).
// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
// IndexKeyFile.delete / RealKeyFile.setDelete at
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5. See CONTRACT.md.
package legacyindex

import "bytes"

// javaDeletedFlag is what RealKeyFile.setDelete(pos, true) writes:
// DataOutputX.writeBoolean(true) is a single 0x01 byte.
const javaDeletedFlag = 1

// Delete marks every live record of the exact key deleted, latest first, the
// way IndexKeyFile.delete does: the bucket chain is walked from its head,
// records whose deleted flag is already nonzero are skipped, and each
// remaining match gets flag byte 1. Nothing else changes: the record stays a
// chain member, the header count is not decremented and no bytes move, so a
// Java reader sees the same file as after its own delete. The number of
// records marked is returned; 0 means the key was not live.
func (i *Index) Delete(key []byte) (int, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.rewritable(); err != nil {
		return 0, err
	}
	bucket, err := i.bucket(key)
	if err != nil {
		return 0, err
	}
	deleted := 0
	err = i.walkChain(bucket, func(m metadata) error {
		if !bytes.Equal(m.Key, key) {
			return nil
		}
		if _, err := i.keys.WriteAt([]byte{javaDeletedFlag}, m.Position); err != nil {
			return err
		}
		deleted++
		return nil
	})
	return deleted, err
}
