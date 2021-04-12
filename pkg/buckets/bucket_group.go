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

type BucketGroup struct {

}

type BucketHandle struct {

}

type Priority = uint64
type Quota = uint64

func (bg *BucketGroup) CreateBucket(initPriority Priority) *BucketHandle {

}

func (bg *BucketGroup) returnQuota(size uint64) {

}

func (bg *BucketGroup) returnBurstQuota(size uint64) {

}
