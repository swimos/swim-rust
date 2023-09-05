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

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use controller::Controller;
use cursive::{Cursive, CursiveExt};
use futures::future::BoxFuture;
use futures::Future;
use model::RuntimeCommand;
use parking_lot::RwLock;
use runtime::dummy_server::make_dummy_runtime;
use runtime::ConsoleFactory;
use shared_state::SharedState;
use swim_utilities::trigger;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use ui::{CursiveUIUpdater, ViewUpdater};

mod controller;
mod data;
mod model;
mod oneshot;
mod runtime;
mod shared_state;
mod ui;

fn main() {
    let mut siv = Cursive::default();

    let shared_state: Arc<RwLock<SharedState>> = Default::default();
    let (command_tx, command_rx) = mpsc::unbounded_channel::<RuntimeCommand>();
    let controller = Controller::new(shared_state.clone(), command_tx, TIMEOUT);

    ui::create_ui(&mut siv, controller, MAX_LINES);
    let (stop_tx, stop_rx) = trigger::trigger();
    let updater = CursiveUIUpdater::new(siv.cb_sink().clone(), TIMEOUT, MAX_LINES);

    let args = std::env::args().collect::<Vec<_>>();
    let runtime = match args.get(1) {
        Some(arg) if args.len() == 2 && arg.as_str() == "--dummy" => {
            make_dummy_runtime(shared_state, command_rx, Arc::new(updater), stop_rx)
        }
        _ => ConsoleFactory.run(shared_state, command_rx, Arc::new(updater), stop_rx),
    };

    let handle = start_runtime(runtime);

    siv.run();
    stop_tx.trigger();
    handle.join().expect("Runtime failed.");
}

const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_LINES: usize = 2048;

fn start_runtime<F: Future<Output = ()> + Send + 'static>(app_runtime: F) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .build()
            .expect("Failed to construct runtime.");

        runtime.block_on(app_runtime);
    })
}

pub trait RuntimeFactory {
    fn run(
        &self,
        shared_state: Arc<RwLock<SharedState>>,
        commands: mpsc::UnboundedReceiver<RuntimeCommand>,
        updater: Arc<dyn ViewUpdater + Send + Sync + 'static>,
        stop: trigger::Receiver,
    ) -> BoxFuture<'static, ()>;
}
