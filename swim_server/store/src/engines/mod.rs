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

// use crate::engines::mem::var::observer::Observer;
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use utilities::sync::topic;

pub mod db;
pub mod mem;

// pub enum StoreStream<T> {
//     Stm(Observer<T>),
//     Db(Arc<Mutex<topic::Sender<T>>>),
// }
//
// impl<T> StoreStream<T> {
//     pub fn stm(delegate: Observer<T>) -> StoreStream<T> {
//         StoreStream::Stm(delegate)
//     }
//
//     pub fn db(delegate: topic::Sender<T>) -> StoreStream<T> {
//         StoreStream::Db(Arc::new(Mutex::new(delegate)))
//     }
// }
//
