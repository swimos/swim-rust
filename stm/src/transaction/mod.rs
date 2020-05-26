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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::ptr::PtrKey;
use crate::var::{TVarInner, VarRef};

#[derive(Debug)]
enum LogState {
    UnconditionalGet,
    UnconditionalSet,
    ConditionalSet(Arc<dyn Any>),
}

impl Default for LogState {
    fn default() -> Self {
        LogState::UnconditionalGet
    }
}

#[derive(Debug)]
struct LogEntry {
    current: Arc<dyn Any + Send + Sync>,
    state: LogState,
}

impl LogEntry {
    fn get_value<T: Any + Send + Sync>(&self) -> Arc<T> {
        let LogEntry { current, .. } = self;
        match current.clone().downcast() {
            Err(_) => panic!(
                "Inconsistent transaction log. Expected {:?} but was {:?}.",
                TypeId::of::<T>(),
                current.type_id()
            ),
            Ok(t) => t,
        }
    }

    fn set<T: Any + Send + Sync>(&mut self, value: T) {
        let old_value = std::mem::replace(&mut self.current, Arc::new(value));
        let old_state = std::mem::take(&mut self.state);
        self.state = match old_state {
            LogState::UnconditionalGet => LogState::ConditionalSet(old_value),
            LogState::UnconditionalSet => LogState::UnconditionalSet,
            cond @ LogState::ConditionalSet(_) => cond,
        };
    }
}

#[derive(Debug, Default)]
pub struct Transaction {
    log: HashMap<PtrKey<Arc<dyn VarRef>>, LogEntry>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            log: HashMap::new(),
        }
    }

    pub(crate) async fn apply_get<T: Any + Send + Sync>(
        &mut self,
        var: &Arc<TVarInner<T>>,
    ) -> Arc<T> {
        let vref: Arc<dyn VarRef> = var.clone();
        let k = PtrKey(vref);
        match self.log.get_mut(&k) {
            Some(entry) => entry.get_value(),
            _ => {
                let value = var.read().await;
                let entry = LogEntry {
                    current: value.clone(),
                    state: LogState::UnconditionalGet,
                };
                self.log.insert(k, entry);
                value
            }
        }
    }

    pub(crate) fn apply_set<T: Any + Send + Sync>(&mut self, var: &Arc<TVarInner<T>>, value: Arc<T>) {
        let vref: Arc<dyn VarRef> = var.clone();
        let k = PtrKey(vref);
        match self.log.get_mut(&k) {
            Some(entry) => {
                entry.set(value);
            }
            _ => {
                let entry = LogEntry {
                    current: value,
                    state: LogState::UnconditionalSet,
                };
                self.log.insert(k, entry);
            }
        }
    }
}

