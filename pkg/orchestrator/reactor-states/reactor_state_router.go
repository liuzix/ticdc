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
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"strings"
)

type ReactorState = orchestrator.ReactorState

type ReactorStateRouter interface {
	RouteForPut(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error)
	RouteForDelete(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error)
}

type ReactorStateStaticRouter struct {
	routes map[util.EtcdRelPrefix]ReactorState
}

func NewReactorStateStaticRouter() *ReactorStateStaticRouter {
	return &ReactorStateStaticRouter{
		routes: make(map[util.EtcdRelPrefix]ReactorState),
	}
}

func (r *ReactorStateStaticRouter) RouteForPut(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error) {
	for prefix, rstate := range r.routes {
		if strings.HasPrefix(key.String(), prefix.String()) {
			return rstate,
		}
	}
}

func (r *ReactorStateStaticRouter) RouteForDelete(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error) {
	panic("implement me")
}

