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
)

// ReactorStateMux combines multiple ReactorState to form one ReactorState.
// Each sub-state is mapped to a unique prefix.
type ReactorStateMux struct {
	substates map[util.EtcdRelPrefix]orchestrator.ReactorState
}

func (s *ReactorStateMux) Update(key util.EtcdRelKey, value []byte) {
	panic("implement me")
}

func (s *ReactorStateMux) GetPatches() []*orchestrator.DataPatch {
	panic("implement me")
}

