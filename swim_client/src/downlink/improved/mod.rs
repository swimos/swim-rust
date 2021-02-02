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

use tokio::sync::mpsc;
use utilities::sync::{topic, promise};
use crate::downlink::{DownlinkError, Message, Command, StateMachine, Event, Operation, DownlinkState, Response, Downlink};
use swim_common::routing::RoutingError;
use swim_common::sink::item::ItemSender;
use std::num::NonZeroUsize;
use crate::configuration::downlink::{OnInvalidMessage, DownlinkParams};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use futures::{StreamExt, select_biased, Stream};
use futures::future::ready;
use futures::stream::once;
use utilities::errors::Recoverable;
use pin_utils::pin_mut;
use tracing::trace;

#[cfg(test)]
mod tests;
pub mod typed;

#[derive(Debug)]
pub struct ImprovedRawDownlink<Act, Ev> {
    action_sender: mpsc::Sender<Act>,
    event_topic: topic::Subscriber<Event<Ev>>,
    completed: Arc<AtomicBool>,
    task_result: promise::Receiver<Result<(), DownlinkError>>,
}

impl<Act, Ev> Clone for ImprovedRawDownlink<Act, Ev> {
    fn clone(&self) -> Self {
        ImprovedRawDownlink {
            action_sender: self.action_sender.clone(),
            event_topic: self.event_topic.clone(),
            task_result: self.task_result.clone(),
            completed: self.completed.clone()
        }
    }
}

impl<Act, Ev> Downlink for ImprovedRawDownlink<Act, Ev> {

    fn is_stopped(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    fn await_stopped(&self) -> promise::Receiver<Result<(), DownlinkError>> {
        self.task_result.clone()
    }

    fn same_downlink(left: &Self, right: &Self) -> bool {
       Arc::ptr_eq(&left.completed, &right.completed)
    }
}

impl<Act, Ev> ImprovedRawDownlink<Act, Ev> {

    pub fn subscriber(&self) -> topic::Subscriber<Event<Ev>> {
        self.event_topic.clone()
    }

    pub fn subscribe(&self) -> Option<topic::Receiver<Event<Ev>>> {
        self.event_topic.subscribe().ok()
    }

    pub async fn send(&self, value: Act) -> Result<(), mpsc::error::SendError<Act>> {
        self.action_sender.send(value).await
    }

    pub fn sender(&self) -> &mpsc::Sender<Act> {
        &self.action_sender
    }

}

#[derive(Clone, Copy, Debug)]
pub struct DownlinkConfig {
    pub buffer_size: NonZeroUsize,
    pub yield_after: NonZeroUsize,
    pub on_invalid: OnInvalidMessage,
}

impl From<&DownlinkParams> for DownlinkConfig {
    fn from(conf: &DownlinkParams) -> Self {
        DownlinkConfig {
            buffer_size: conf.buffer_size,
            yield_after: conf.yield_after,
            on_invalid: conf.on_invalid,
        }
    }
}

pub(in crate::downlink) fn create_downlink<M, Act, State, Machine, CmdSend, Updates>(
    machine: Machine,
    update_stream: Updates,
    cmd_sink: CmdSend,
    config: DownlinkConfig,
) -> (ImprovedRawDownlink<Act, Machine::Ev>, topic::Receiver<Event<Machine::Ev>>)
where
    M: Send + 'static,
    Act: Send + 'static,
    State: Send + 'static,
    Machine: StateMachine<State, M, Act> + Send + Sync + 'static,
    Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + Sync + 'static,
    CmdSend: ItemSender<Command<Machine::Cmd>, RoutingError> + Send + Sync + 'static,
{
    let (act_tx, act_rx) = mpsc::channel::<Act>(config.buffer_size.get());
    let (event_tx, event_rx) = topic::channel::<Event<Machine::Ev>>(config.buffer_size);

    let (stopped_tx, stopped_rx) = promise::promise();

    let completed = Arc::new(AtomicBool::new(false));
    let completed_cpy = completed.clone();

    // The task that maintains the internal state of the lane.
    let task: ImprovedDownlinkTask<Act, Machine::Ev> = ImprovedDownlinkTask {
        event_sink: event_tx,
        actions: act_rx,
        config,
    };

    let dl_task = async move {
        let result = task.run(machine, update_stream, cmd_sink).await;
        completed.store(true, Ordering::Release);
        let _ = stopped_tx.provide(result);
    };

    swim_runtime::task::spawn(dl_task);

    let sub = event_rx.subscriber();

    let dl = ImprovedRawDownlink {
        action_sender: act_tx,
        event_topic: sub,
        task_result: stopped_rx,
        completed: completed_cpy,
    };
    (dl, event_rx)
}

struct ImprovedDownlinkTask<Act, Ev> {
    event_sink: topic::Sender<Event<Ev>>,
    actions: mpsc::Receiver<Act>,
    config: DownlinkConfig,
}

impl<Act, Ev> ImprovedDownlinkTask<Act, Ev> {

    async fn run<M, Cmd, State, Updates>(self,
                             state_machine: impl StateMachine<State, M, Act, Cmd = Cmd, Ev = Ev>,
                             updates: Updates,
                             mut cmd_sink: impl ItemSender<Command<Cmd>, RoutingError>) -> Result<(), DownlinkError>
    where
        Updates: Stream<Item = Result<Message<M>, RoutingError>> + Send + Sync + 'static,
    {
        let ImprovedDownlinkTask {
            mut event_sink,
            actions,
            config } = self;

        let update_ops = updates.map(|r| match r {
            Ok(upd) => Operation::Message(upd),
            Err(e) => Operation::Error(e),
        });

        let start: Operation<M, Act> = Operation::Start;
        let ops = once(ready(start)).chain(update_ops);

        let mut dl_state = DownlinkState::Unlinked;
        let mut model = state_machine.init_state();
        let yield_mod = config.yield_after.get();

        let ops = ops.fuse();
        let actions = actions.fuse();

        pin_mut!(ops);
        pin_mut!(actions);

        let mut act_terminated = false;
        let mut events_terminated = false;
        let mut read_act = false;

        let mut iteration_count: usize = 0;

        trace!("Running downlink task");

        let result: Result<(), DownlinkError> = loop {
            let next_op: TaskInput<M, Act> =
                if dl_state == state_machine.dl_start_state() && !act_terminated {
                    if read_act {
                        read_act = false;
                        let input = select_biased! {
                            act_op = actions.next() => TaskInput::from_action(act_op),
                            upd_op = ops.next() => TaskInput::from_operation(upd_op),
                        };
                        input
                    } else {
                        read_act = true;
                        let input = select_biased! {
                            upd_op = ops.next() => TaskInput::from_operation(upd_op),
                            act_op = actions.next() => TaskInput::from_action(act_op),
                        };
                        input
                    }
                } else {
                    TaskInput::from_operation(ops.next().await)
                };

            match next_op {
                TaskInput::Op(op) => {
                    let Response {
                        event,
                        command,
                        error,
                        terminate,
                    } = match state_machine.handle_operation(&mut dl_state, &mut model, op) {
                        Ok(r) => r,
                        Err(e) => match e {
                            e @ DownlinkError::TaskPanic(_) => {
                                break Err(e);
                            }
                            _ => match config.on_invalid {
                                OnInvalidMessage::Ignore => {
                                    continue;
                                }
                                OnInvalidMessage::Terminate => {
                                    break Err(e);
                                }
                            },
                        },
                    };
                    let result = match (event, command) {
                        (Some(event), Some(cmd)) => {
                            if !events_terminated && event_sink.discarding_send(event).await.is_err() {
                                events_terminated = true;
                            }
                            cmd_sink.send_item(cmd).await
                        }
                        (Some(event), _) => {
                            if !events_terminated && event_sink.discarding_send(event).await.is_err() {
                                events_terminated = true;
                            }
                            Ok(())
                        }
                        (_, Some(command)) => cmd_sink.send_item(command).await,
                        _ => Ok(()),
                    };

                    if error.map(|e| e.is_fatal()).unwrap_or(false) {
                        break Err(DownlinkError::TransitionError);
                    } else if terminate || result.is_err() {
                        break result.map_err(Into::into);
                    } else if act_terminated && events_terminated {
                        break Ok(());
                    }
                }
                TaskInput::ActTerminated => {
                    act_terminated = true;
                    if events_terminated {
                        break Ok(());
                    }
                }
                TaskInput::Terminated => {
                    break Ok(());
                }
            }

            iteration_count += 1;
            if iteration_count % yield_mod == 0 {
                tokio::task::yield_now().await;
            }
        };
        let _ = cmd_sink.send_item(Command::Unlink).await;
        result
    }

}


enum TaskInput<M, A> {
    Op(Operation<M, A>),
    ActTerminated,
    Terminated,
}

impl<M, A> TaskInput<M, A> {
    fn from_action(maybe_action: Option<A>) -> TaskInput<M, A> {
        match maybe_action {
            Some(a) => TaskInput::Op(Operation::Action(a)),
            _ => TaskInput::ActTerminated,
        }
    }

    fn from_operation(maybe_action: Option<Operation<M, A>>) -> TaskInput<M, A> {
        match maybe_action {
            Some(op) => TaskInput::Op(op),
            _ => TaskInput::Terminated,
        }
    }
}
