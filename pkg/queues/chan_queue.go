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

package queues

import (
	"log"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type ChanQueue struct {
	ch           chan interface{}
	isRecvClosed int32 // atomic
	recvCloseCh  chan struct{}
}

func (c *ChanQueue) Send(ctx context.Context, value interface{}) error {
	if context.IsAsync(ctx) {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case c.ch <- value:
			return nil
		default:
			return cerror.ErrWouldBlock.FastGenByArgs()
		}
	}

	// We are in a synchronous context
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case c.ch <- value:
	case <-c.recvCloseCh:
		return cerror.ErrQueueClosed.GenWithStackByArgs()
	}
	return nil
}

func (c *ChanQueue) CloseSend() error {
	close(c.ch)
	return nil
}

func (c *ChanQueue) Receive(ctx context.Context) (interface{}, error) {
	if atomic.LoadInt32(&c.isRecvClosed) != 0 {
		log.Panic("receiving on a closed receive end", zap.Stack("stack"))
	}

	if context.IsAsync(ctx) {
		select {
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		case val, ok := <-c.ch:
			if !ok {
				return nil, cerror.ErrQueueClosed.GenWithStackByArgs()
			}
			return val, nil
		default:
			return nil, cerror.ErrWouldBlock.FastGenByArgs()
		}
	}

	// We are in a synchronous context
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case val, ok := <-c.ch:
		if !ok {
			return nil, cerror.ErrQueueClosed.GenWithStackByArgs()
		}
		return val, nil
	}
}

func (c *ChanQueue) CloseReceive() error {
	if !atomic.CompareAndSwapInt32(&c.isRecvClosed, 0, 1) {
		// returns an error on duplicate close
		return cerror.ErrQueueClosed.GenWithStackByArgs()
	}
	close(c.recvCloseCh)
	return nil
}
