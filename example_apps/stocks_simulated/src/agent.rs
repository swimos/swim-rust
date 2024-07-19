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

use futures::stream::unfold;
use futures::StreamExt;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::time::Duration;
use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt};
use swimos::agent::lanes::{CommandLane, JoinValueLane, ValueLane};
use swimos::agent::{lifecycle, projections, AgentLaneModel};
use swimos::model::Timestamp;
use swimos_form::Form;
use tokio::time::sleep;
use tracing::info;

/// Agent for tracking all the stocks.
#[projections]
#[derive(AgentLaneModel)]
pub struct SymbolsAgent {
    /// Lane for receiving stocks to track.
    add: CommandLane<String>,
    /// Stock downlinks.
    stocks: JoinValueLane<String, Stock>,
}

/// Symbols Agent Lifecycle.
#[derive(Clone)]
pub struct SymbolsLifecycle;

#[lifecycle(SymbolsAgent)]
impl SymbolsLifecycle {
    /// Lifecycle event handler which is invoked exactly once when `add` lane receives a command to
    /// add a downlink to `id`.
    #[on_command(add)]
    pub fn add(
        &self,
        context: HandlerContext<SymbolsAgent>,
        id: &str,
    ) -> impl EventHandler<SymbolsAgent> {
        let node = format!("/stock/{id}");
        context.add_downlink(
            SymbolsAgent::STOCKS,
            id.to_string(),
            None,
            node.as_str(),
            "status",
        )
    }
}

/// Stock Agent implementation.
#[projections]
#[derive(AgentLaneModel)]
pub struct StockAgent {
    /// The state of the current stock.
    status: ValueLane<Stock>,
}

/// Stock model.
#[derive(Form, Clone)]
pub struct Stock {
    /// The timestamp that the stock was last updated.
    timestamp: Timestamp,
    /// The stock's current price.
    price: f64,
    /// The stock's current volume.
    volume: f64,
    /// The stock's current bid.
    bid: f64,
    /// The stock's current add.
    ask: f64,
    /// The stock's movement since it was last updated.
    movement: f64,
}

impl Default for Stock {
    fn default() -> Self {
        Stock::select_random(0.0)
    }
}

impl Stock {
    /// Generates a stock. Tracking its movement since `previous_price`.
    fn select_random(previous_price: f64) -> Stock {
        let mut rng = ThreadRng::default();
        let current_price = truncate(rng.gen());

        Stock {
            timestamp: Timestamp::now(),
            price: current_price,
            volume: truncate(rng.gen::<f64>() * 400.0).powi(3),
            bid: truncate(rng.gen::<f64>() * 100.0),
            ask: truncate(rng.gen::<f64>() * 100.0),
            movement: current_price - previous_price,
        }
    }
}

/// Truncates `f` to two decimal places.
fn truncate(f: f64) -> f64 {
    (f * 100.0).round() / 100.0
}

/// Stock Agent's lifecycle implementation.
#[derive(Clone)]
pub struct StockLifecycle;

#[lifecycle(StockAgent)]
impl StockLifecycle {
    /// Lifecycle event handler which is invoked exactly once when the agent starts.
    ///
    /// The handler will spawn a stream which will generate random stocks and send a command to the
    /// `SymbolsAgent` to add a downlink to this agent.
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<StockAgent>) -> impl EventHandler<StockAgent> {
        let stream = unfold(Duration::default(), move |delay| async move {
            sleep(delay).await;

            let mut rng = ThreadRng::default();
            let handler = generate_stock(context);
            let next_delay = Duration::from_secs(rng.gen_range(5..=10));

            Some((handler, next_delay))
        });
        context
            .get_parameter("symbol")
            .and_then(move |symbol_opt: Option<String>| {
                let symbol = symbol_opt.expect("Missing symbol for stock");
                context.send_command(None, "/symbols", "add", symbol)
            })
            .followed_by(context.suspend_schedule(stream.boxed()))
            .followed_by(context.get_agent_uri())
            .and_then(move |uri| context.effect(move || info!(%uri, "Started agent")))
    }
}

/// Returns an event handler which will generate a new stock and set it to the `status` lane.
fn generate_stock(context: HandlerContext<StockAgent>) -> impl EventHandler<StockAgent> {
    context
        .get_value(StockAgent::STATUS)
        .and_then(move |previous_stock: Stock| {
            context.set_value(
                StockAgent::STATUS,
                Stock::select_random(previous_stock.price),
            )
        })
}
