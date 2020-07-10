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

use crate::agent::context::ContextImpl;
use crate::agent::AgentContext;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use url::Url;

#[test]
fn simple_accessors() {
    let (tx, _rx) = mpsc::channel(1);
    let agent = Arc::new("agent");
    let url: Url = Url::parse("swim://host/node").unwrap();
    let context = ContextImpl::new(agent.clone(), url.clone(), tx);
    assert!(std::ptr::eq(context.agent(), agent.as_ref()));
    assert_eq!(context.node_url(), &url);
}

fn create_context(n: usize) -> ContextImpl<&'static str> {
    let (tx, rx) = mpsc::channel(n);

    //Run any tasks that get scheduled.
    swim_runtime::task::spawn(async move { rx.for_each(|eff| eff).await });

    let agent = Arc::new("agent");
    let url: Url = Url::parse("swim://host/node").unwrap();
    ContextImpl::new(agent.clone(), url.clone(), tx)
}

#[tokio::test]
async fn send_single_to_scheduler() {
    let context = create_context(1);

    let (mut defer_tx, mut defer_rx) = mpsc::channel(1);
    context
        .defer(
            async move {
                let _ = defer_tx.send(6).await;
            },
            Duration::from_micros(50),
        )
        .await;

    let result = defer_rx.recv().await;

    assert_eq!(result, Some(6));
}

#[tokio::test]
async fn send_multiple_to_scheduler() {
    let context = create_context(1);

    let (defer_tx, mut defer_rx) = mpsc::channel(1);
    let mut i = 0;
    context
        .periodically(
            move || {
                let mut tx = defer_tx.clone();
                i += 1;
                let c = i;
                async move {
                    let _ = tx.send(c).await;
                }
            },
            Duration::from_micros(50),
            Some(3),
        )
        .await;

    let result = defer_rx.recv().await;
    assert_eq!(result, Some(1));
    let result = defer_rx.recv().await;
    assert_eq!(result, Some(2));
    let result = defer_rx.recv().await;
    assert_eq!(result, Some(3));
}
