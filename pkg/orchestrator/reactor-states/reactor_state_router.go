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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type ReactorState = orchestrator.ReactorState

type ReactorStateRouter interface {
	RouteForPut(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error)
	RouteForDelete(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error)
}

type ReactorStateSingletonRouter struct {
	state ReactorState
}

func (r *ReactorStateSingletonRouter) RouteForPut(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error) {

}

func (r *ReactorStateSingletonRouter) RouteForDelete(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error) {
	panic("implement me")
}

type ReactorStateStaticRouter struct {
	routes map[util.EtcdRelPrefix]ReactorStateRouter
}

func NewReactorStateStaticRouter() *ReactorStateStaticRouter {
	return &ReactorStateStaticRouter{
		routes: make(map[util.EtcdRelPrefix]ReactorStateRouter),
	}
}

// AddRoute registers a sub-router with the specified prefix.
func (r *ReactorStateStaticRouter) AddRoute(prefix util.EtcdRelPrefix, router ReactorStateRouter) {
	r.routes[prefix] = router
}

func (r *ReactorStateStaticRouter) AddState(prefix util.EtcdRelPrefix, state ReactorState) {

}

func (r *ReactorStateStaticRouter) RouteForPut(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error) {
	for prefix, nextRouter := range r.routes {
		if strings.HasPrefix(key.String(), prefix.String()) {
			rstate, rkey, err := nextRouter.RouteForPut(key.RemovePrefix(&prefix))
			if err != nil {
				return nil, util.EtcdRelKey{}, errors.Trace(err)
			}
			return rstate, rkey, nil
		}
	}
	return nil, util.EtcdRelKey{}, errors.Errorf("ReactorStateStaticRouter: no matching prefix for key %s", key.String())
}

func (r *ReactorStateStaticRouter) RouteForDelete(key util.EtcdRelKey) (ReactorState, util.EtcdRelKey, error) {
	for prefix, nextRouter := range r.routes {
		if strings.HasPrefix(key.String(), prefix.String()) {
			rstate, rkey, err := nextRouter.RouteForDelete(key.RemovePrefix(&prefix))
			if err != nil {
				return nil, util.EtcdRelKey{}, errors.Trace(err)
			}
			return rstate, rkey, nil
		}
	}
	return nil, util.EtcdRelKey{}, errors.Errorf("ReactorStateStaticRouter: no matching prefix for key %s", key.String())
}

