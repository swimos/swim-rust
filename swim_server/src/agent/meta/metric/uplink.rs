// Copyright 2015-2020 SWIM.AI inc.
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

use std::time::{Duration, Instant};

use swim_common::form::Form;
use swim_common::warp::path::RelativePath;

use crate::agent::meta::metric::sender::{Surjection, TransformedSender};
use crate::agent::meta::metric::ObserverEvent;
use std::mem::replace;

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct UplinkUplinkProfile {
    pub event_delta: i32,
    pub event_rate: u64,
    pub event_count: u64,
    pub command_delta: i32,
    pub command_rate: u64,
    pub command_count: u64,
}

impl UplinkUplinkProfile {
    fn build(&mut self, last_report: &Instant) {
        let UplinkUplinkProfile {
            event_delta,
            event_rate,
            event_count,
            command_delta,
            command_rate,
            command_count,
        } = self;

        let now = Instant::now();
        let dt = now.duration_since(*last_report).as_millis() as u64;

        let event_delta = replace(event_delta, 0);
        *event_rate = ((event_delta * 1000) as u64 / dt) as u64;
        *event_count = event_count.wrapping_add(event_delta as u64);

        let command_delta = replace(command_delta, 0);
        *command_rate = ((command_delta * 1000) as u64 / dt) as u64;
        *command_count = command_count.wrapping_add(command_delta as u64);
    }
}

pub struct UplinkSurjection(pub RelativePath);
impl Surjection<UplinkUplinkProfile> for UplinkSurjection {
    fn onto(&self, input: UplinkUplinkProfile) -> ObserverEvent {
        ObserverEvent::Uplink(self.0.clone(), input)
    }
}

pub struct UplinkObserver {
    sender: TransformedSender<UplinkSurjection, UplinkUplinkProfile>,
    last_report: Instant,
    report_interval: Duration,
    profile: UplinkUplinkProfile,
}

impl UplinkObserver {
    pub fn new(
        sender: TransformedSender<UplinkSurjection, UplinkUplinkProfile>,
        report_interval: Duration,
    ) -> UplinkObserver {
        UplinkObserver {
            sender,
            last_report: Instant::now(),
            report_interval,
            profile: UplinkUplinkProfile::default(),
        }
    }

    fn flush(&mut self) {
        let UplinkObserver {
            sender,
            last_report,
            report_interval,
            profile,
        } = self;

        if last_report.elapsed() > *report_interval {
            profile.build(last_report);

            match sender.try_send(profile.clone()) {
                Ok(()) => {
                    *last_report = Instant::now();
                }
                Err(_) => {
                    // agent has been stopped or a capacity limit has been reached
                }
            }
        }
    }

    pub fn on_event(&mut self) {
        self.profile.event_count = self.profile.event_count.wrapping_add(1);
        self.flush();
    }

    pub fn on_command(&mut self) {
        self.profile.command_count = self.profile.command_count.wrapping_add(1);
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_uplink_surjection() {
        let path = RelativePath::new("/node", "/lane");
        let (tx, mut rx) = mpsc::channel(1);
        let sender = TransformedSender::new(UplinkSurjection(path.clone()), tx);
        let profile = UplinkUplinkProfile::default();

        assert!(sender.try_send(profile.clone()).is_ok());
        assert_eq!(
            rx.recv().now_or_never().unwrap().unwrap(),
            ObserverEvent::Uplink(path, profile)
        );
    }
}
