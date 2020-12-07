// Copyright 2020 PingCAP, Inc.
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

package reactor_states

import (
	"github.com/dghubble/trie"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type ReactorState = orchestrator.ReactorState

type ReactorStateRouter interface {
	RouteForPut(key util.EtcdKey) (ReactorState, util.EtcdRelKey, error)
	RouteForDelete(key util.EtcdKey) (ReactorState, util.EtcdRelKey, error)
}

type ReactorStateStaticRouter struct {
	routes *trie.PathTrie
}

func NewReactorStateStaticRouter() *ReactorStateStaticRouter {
	return &ReactorStateStaticRouter{
		routes: trie.NewPathTrie(),
	}
}

func (r *ReactorStateStaticRouter) RouteForPut(key util.EtcdKey) (ReactorState, util.EtcdRelKey, error) {
	
}

func (r *ReactorStateStaticRouter) RouteForDelete(key util.EtcdKey) (ReactorState, util.EtcdRelKey, error) {
	panic("implement me")
}

