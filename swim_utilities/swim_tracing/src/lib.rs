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

#[cfg(feature = "future")]
pub mod request;

use std::sync::Once;
use tracing::Level;
use tracing_subscriber::EnvFilter;

static INIT: Once = Once::new();

/// Add a tracing subscriber for the given directives to see their trace messages.
///
/// # Arguments
///
/// * `directives`             - The trace spans that you want to see messages from.
/// * `level`                  - The trace level that you want to see messages from.
#[cfg(not(tarpaulin_include))]
pub fn init_trace(directives: Vec<&str>, level: Level) {
    INIT.call_once(|| {
        if directives.is_empty() {
            tracing_subscriber::fmt().with_max_level(level).init();
        } else {
            let mut filter = EnvFilter::from_default_env();

            for directive in directives {
                filter = filter.add_directive(directive.parse().unwrap());
            }

            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_env_filter(filter)
                .init();
        }
    });
}
