// Copyright 2015-2021 Swim Inc.
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
use swim_api::meta::lane::LaneKind;
use swim_model::Text;
use swim_runtime::agent::reporting::UplinkReportReader;

#[derive(Debug, Clone)]
pub struct LaneView {
    kind: LaneKind,
    report_reader: UplinkReportReader,
}

#[derive(Debug)]
pub struct AgentSnapshot {
    lanes: HashMap<Text, LaneView>,
    aggregate_reporter: UplinkReportReader,
}

#[derive(Debug)]
struct Inner {
    aggregate_reporter: UplinkReportReader,
    lanes: Mutex<HashMap<Text, LaneView>>,
    epoch: AtomicU64,
}

#[derive(Debug)]
pub struct AgentIntrospectionUpdater {
    inner: Arc<Inner>,
}

pub fn agent_introspection(
    aggregate_reporter: UplinkReportReader,
) -> (AgentIntrospectionUpdater, AgentIntrospectionHandle) {
    let inner = Arc::new(Inner {
        aggregate_reporter,
        lanes: Default::default(),
        epoch: AtomicU64::new(0),
    });
    (
        AgentIntrospectionUpdater {
            inner: inner.clone(),
        },
        AgentIntrospectionHandle {
            current_epoch: 0,
            inner,
        },
    )
}

impl AgentIntrospectionUpdater {
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
}

#[derive(Debug, Clone)]
pub struct AgentIntrospectionHandle {
    current_epoch: u64,
    inner: Arc<Inner>,
}

impl AgentIntrospectionHandle {
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
            let lanes_cpy = clear_closed(&mut *guard);
            *current_epoch = epoch.load(Ordering::Relaxed);
            Some(AgentSnapshot {
                lanes: lanes_cpy,
                aggregate_reporter: aggregate_reporter.clone(),
            })
        } else {
            None
        }
    }

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
}

fn clear_closed(lanes: &mut HashMap<Text, LaneView>) -> HashMap<Text, LaneView> {
    lanes.retain(|_, v| v.report_reader.is_active());
    lanes.clone()
}
