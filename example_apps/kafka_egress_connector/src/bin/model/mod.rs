// Copyright 2015-2024 Swim Inc.
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

use std::time::SystemTime;

use rand::Rng;
use swimos_form::Form;

#[derive(Debug, Clone, Copy, Form)]
pub struct Status {
    pub severity: f64,
}

#[derive(Debug, Clone, Copy, Form)]
pub struct Run {
    pub mean_ul_sinr: i32,
    pub rrc_re_establishment_failures: u32,
    pub recorded_time: u64,
}

#[derive(Debug, Clone, Copy, Form)]
pub struct Event {
    pub status: Status,
    #[form(name = "ranLatest")]
    pub ran_latest: Run,
}

#[derive(Debug, Clone, Copy, Form)]
pub struct EventWithKey {
    pub key: i32,
    pub payload: Event,
}

pub fn random_record() -> EventWithKey {
    let mut rng = rand::thread_rng();
    let severity = rng.gen_range(0.0..2.0);
    let mean_ul_sinr = rng.gen_range(1..50);
    let rrc_re_establishment_failures = rng.gen_range(1u32..10u32);

    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Time out of range.");
    let recorded_time = t.as_secs() * 1000 + t.subsec_millis() as u64;

    let status = Status { severity };
    let ran_latest = Run {
        mean_ul_sinr,
        rrc_re_establishment_failures,
        recorded_time,
    };

    let key = rng.gen_range(50..4000);
    EventWithKey {
        key,
        payload: Event { status, ran_latest },
    }
}
