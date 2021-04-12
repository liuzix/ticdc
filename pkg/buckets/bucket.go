// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package buckets

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/trees/redblacktree"
)

type bucketID = uint64

type bucket struct {
	ID bucketID

	Priority   Priority
	Quota      Quota
	BurstQuota Quota

	bg *bucketGroup
}

func (b *bucket) adjustPriority(p Priority) {
	b.bg.rwLock.Lock()
	defer b.bg.rwLock.Unlock()

	b.bg.tree.Remove(b.ID)

	oldPriority := atomic.SwapUint64(&b.Priority, p)
	if oldPriority > p {
		log.Panic("priority regressed",
			zap.Uint64("old", oldPriority),
			zap.Uint64("new", p))
	}

	b.bg.tree.Put(b, struct{}{})
}

func (b *bucket)

type bucketGroup struct {
	rwLock sync.RWMutex
	tree   *redblacktree.Tree
}

func newBucketGroup() *bucketGroup {
	comparator := func(a, b interface{}) int {
		first := a.(*bucket)
		second := b.(*bucket)

		p1 := atomic.LoadUint64(&first.Priority)
		p2 := atomic.LoadUint64(&second.Priority)

		// We don't perform a subtraction to type conversion and overflow
		if p1 < p2 {
			return -1
		} else if p1 == p2 {
			return 0
		} else {
			return 1
		}
	}

	return &bucketGroup{tree: redblacktree.NewWith(comparator)}
}
