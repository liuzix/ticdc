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

import "container/heap"
import "sync/atomic"

type bucket struct {
	priority Priority
}

func (b *bucket) getPriority() Priority {
	return atomic.LoadUint64(&b.priority)
}

func (b *bucket) setPriority(p Priority) {
	atomic.StoreUint64(&b.priority, p)
}

type bucketHeap struct {
	arr []*bucket
}

func (h *bucketHeap) Len() int {
	return len(b.arr)
}

func (h *bucketHeap) Less(i, j int) bool {
	b1 := h.arr[i]
	b2 := h.arr[j]

	return b1.getPriority() < b2.getPriority()
}

func (h *bucketHeap) Swap(i, j int) {
	panic("implement me")
}

func (h *bucketHeap) Push(x interface{}) {
	panic("implement me")
}

func (h *bucketHeap) Pop() interface{} {
	panic("implement me")
}





