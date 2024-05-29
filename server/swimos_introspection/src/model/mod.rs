// Copyright 2015-2023 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use swimos_api::agent::LaneKind;
use swimos_meta::LaneInfo;
use swimos_model::Text;
use swimos_runtime::agent::reporting::UplinkReportReader;

#[cfg(test)]
mod tests;

/// View of a lane for the introspection meta-agents. Reports the kind of the lane and
/// allows snapshots of the uplink statistics to be generated.
#[derive(Debug, Clone)]
pub struct LaneView {
    pub kind: LaneKind,
    pub report_reader: UplinkReportReader,
}

impl LaneView {
    pub fn new(kind: LaneKind, report_reader: UplinkReportReader) -> Self {
        LaneView {
            kind,
            report_reader,
        }
    }
}

/// Snapshot of the state of an agent for the node meta-agent.
#[derive(Debug)]
pub struct AgentSnapshot {
    /// Views for each lane of the agent.
    pub lanes: HashMap<Text, LaneView>,
    /// Reader to obtain aggregate uplink statistics for the node.
    pub aggregate_reporter: UplinkReportReader,
}

impl AgentSnapshot {
    /// Iterate over all lanes in an agent snapshot.
    pub fn lane_info(&self) -> impl Iterator<Item = LaneInfo> + '_ {
        self.lanes
            .iter()
            .map(|(name, view)| LaneInfo::new(name.clone(), view.kind))
    }
}

#[derive(Debug)]
struct Inner {
    aggregate_reporter: UplinkReportReader,
    lanes: Mutex<HashMap<Text, LaneView>>,
    epoch: AtomicU64,
}

/// Used to keep track of the lanes associated with an agent.
#[derive(Debug)]
pub struct AgentIntrospectionUpdater {
    inner: Arc<Inner>,
}

impl AgentIntrospectionUpdater {
    pub fn new(aggregate_reporter: UplinkReportReader) -> Self {
        let inner = Arc::new(Inner {
            aggregate_reporter,
            lanes: Default::default(),
            epoch: AtomicU64::new(0),
        });
        AgentIntrospectionUpdater { inner }
    }
}

impl AgentIntrospectionUpdater {
    /// Add a new lane for the agent.
    ///
    /// #Arguments
    /// * `name` - The name of the lane.
    /// * `kind` - The kind of the lane.
    /// * `report_reader` - Reader for uplink statistics snapshots for the lane.
    pub fn add_lane(&self, name: Text, kind: LaneKind, report_reader: UplinkReportReader) {
        let Inner { lanes, epoch, .. } = &*self.inner;
        let mut guard = lanes.lock();
        guard.insert(
            name,
            LaneView {
                kind,
                report_reader,
            },
        );
        epoch.fetch_add(1, Ordering::Relaxed);
    }

    /// Create an introspection handle for a meta-agent.
    pub fn make_handle(&self) -> AgentIntrospectionHandle {
        AgentIntrospectionHandle {
            current_epoch: 0,
            inner: self.inner.clone(),
        }
    }
}

/// A handle used by a meta-agent to observe the state of its associated agent.
#[derive(Debug, Clone)]
pub struct AgentIntrospectionHandle {
    current_epoch: u64,
    inner: Arc<Inner>,
}

impl AgentIntrospectionHandle {
    /// Attempt to create a new snapshot of the state of the agent. If the agent has stopped
    /// this will return nothing.
    pub fn new_snapshot(&mut self) -> Option<AgentSnapshot> {
        let AgentIntrospectionHandle {
            inner,
            current_epoch,
        } = self;
        let Inner {
            lanes,
            epoch,
            aggregate_reporter,
        } = &**inner;
        if aggregate_reporter.is_active() {
            let mut guard = lanes.lock();
            let lanes_cpy = clear_closed(&mut guard);
            *current_epoch = epoch.load(Ordering::Relaxed);
            Some(AgentSnapshot {
                lanes: lanes_cpy,
                aggregate_reporter: aggregate_reporter.clone(),
            })
        } else {
            None
        }
    }

    /// Determine if the lanes of the agent have changed (tracked by the epoch counter). If this is the
    /// case, a new snapshot is required.
    pub fn changed(&self) -> bool {
        let AgentIntrospectionHandle {
            inner,
            current_epoch,
        } = self;
        let Inner {
            epoch,
            aggregate_reporter,
            ..
        } = &**inner;
        !aggregate_reporter.is_active() || epoch.load(Ordering::Relaxed) != *current_epoch
    }

    /// Create a reader for the aggreate uplink statistics.
    pub fn aggregate_reader(&self) -> UplinkReportReader {
        self.inner.aggregate_reporter.clone()
    }
}

// Clear any lanes that have stopped running when producing a new snapshot.
fn clear_closed(lanes: &mut HashMap<Text, LaneView>) -> HashMap<Text, LaneView> {
    lanes.retain(|_, v| v.report_reader.is_active());
    lanes.clone()
}
