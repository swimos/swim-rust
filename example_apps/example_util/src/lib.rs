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
    fmt::{Display, Formatter},
};

use swim::server::ServerHandle;
use tokio::select;

pub async fn manage_handle(mut handle: ServerHandle) {
    let mut shutdown_hook = Box::pin(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register interrupt handler.");
    });
    let print_addr = handle.bound_addr();

    let maybe_addr = select! {
        _ = &mut shutdown_hook => None,
        maybe_addr = print_addr => maybe_addr,
    };

    if let Some(addr) = maybe_addr {
        println!("Bound to: {}", addr);
        shutdown_hook.await;
    }

    println!("Stopping server.");
    handle.stop();
}

struct FormatMap<'a, K, V>(&'a HashMap<K, V>);

pub fn format_map<K: Display, V: Display>(map: &HashMap<K, V>) -> impl Display + '_ {
    FormatMap(map)
}

impl<'a, K: Display, V: Display> Display for FormatMap<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut it = self.0.iter();
        if let Some((k, v)) = it.next() {
            write!(f, " ({}, {})", k, v)?;
            for (k, v) in it {
                write!(f, ", ({}, {})", k, v)?;
            }
            write!(f, " ")?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}
