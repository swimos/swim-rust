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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::stream::unfold;
use futures::StreamExt;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos::agent::lanes::{CommandLane, MapLane, ValueLane};
use swimos::agent::{lifecycle, projections, AgentLaneModel};
use swimos::model::Timestamp;

use swimos_form::Form;
use tokio::time::sleep;
use tracing::{info, trace};

const TAG: &str = "swim0";
type Rand = Arc<Mutex<StdRng>>;

/// Ripple identifier.
#[derive(Clone, Debug, Form, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum Id {
    /// System-generated event.
    System(String),
    /// User-generated event. Tuple of a base64 encoded byte array and a string containing "mouse".
    User(String, String),
}

/// Ripple configuration.
#[derive(Copy, Clone, Debug, Form)]
#[form(fields_convention = "camel")]
pub struct Config {
    /// The minimum number of ripples to generate for an event.
    min_phases: usize,
    /// The maximum number of ripples to generate for an event.
    max_phases: usize,
    /// The maximum time that a ripple should stay in the UI.
    ripple_duration: usize,
    /// Ripple ease time.
    ripple_spread: usize,
}

impl Config {
    /// Default minimum number of phases to generate for a ripple.
    pub const MIN_PHASES: usize = 2;
    /// Default maximum number of phases to generate for a ripple.
    pub const MAX_PHASES: usize = 5;
    /// Minimum delay to wait between generating ripples.
    pub const MIN_DELAY: u64 = 500;
    /// Maximum delay to wait between generating ripples.
    pub const MAX_DELAY: u64 = 2000;
}

impl Default for Config {
    fn default() -> Self {
        Config {
            min_phases: Self::MIN_PHASES,
            max_phases: Self::MAX_PHASES,
            ripple_duration: 5000,
            ripple_spread: 3000,
        }
    }
}

/// Web Agent for mirroring ripples.
#[derive(AgentLaneModel)]
#[projections]
pub struct MirrorAgent {
    /// The current configuration for the universe.
    mode: ValueLane<Config>,
    /// Lane for receiving commands to publish new ripples.
    ripple: CommandLane<Ripple>,
    /// Lane for propagating the most recent ripple.
    ripples: ValueLane<Ripple>,
    /// Lane for receiving commands to publish new charges.
    charge: CommandLane<ChargeAction>,
    /// Lane for propagating charges.
    charges: MapLane<Id, Charge>,
}

/// A charge model.
///
/// A charge is an event generated when a user is holding their left mouse button down and moving it
/// around the screen.
#[derive(Debug, Form, Clone)]
pub struct Charge {
    /// The ID of the user which is generating the charge.
    id: Id,
    /// The charge's current X position.
    x: f64,
    /// The charge's current Y position.
    y: f64,
    /// The color of the charge.
    color: String,
    /// The radius of the charge.
    #[form(name = "r")]
    radius: i32,
    /// The time which the charge was created.
    #[form(name = "t0")]
    created: Timestamp,
    /// The time which the charge was last updated.
    #[form(name = "t")]
    updated: Timestamp,
}

/// A charge event action.
#[derive(Debug, Form, Clone)]
pub enum ChargeAction {
    /// Create a new charge event.
    Create {
        /// The ID of the user which is generating the charge.
        id: Id,
        /// The charge's current X position.
        x: f64,
        /// The charge's current Y position.
        y: f64,
        /// The color of the charge.
        color: String,
        /// The radius of the charge.
        #[form(name = "r")]
        radius: i32,
    },
    /// Move the charge to the new position. Updating the last updated timestamp to ensure that it
    /// is not pruned.
    Move {
        /// The ID of the user which is generating the charge.
        id: Id,
        /// The charge's current X position.
        x: f64,
        /// The charge's current Y position.
        y: f64,
        /// The color of the charge.
        color: String,
        /// The radius of the charge.
        #[form(name = "r")]
        radius: i32,
    },
    /// Remove the charge.
    Remove {
        /// The ID of the charge to remove.
        id: Id,
    },
}

/// The agent's lifecycle.
#[derive(Clone)]
pub struct MirrorLifecycle {
    /// The random number generator to use.
    rng: Arc<Mutex<StdRng>>,
}

impl MirrorLifecycle {
    /// Creates a new lifecycle which will use `rng` when generating events.
    pub fn new(rng: StdRng) -> MirrorLifecycle {
        MirrorLifecycle {
            rng: Arc::new(Mutex::new(rng)),
        }
    }
}

#[lifecycle(MirrorAgent)]
impl MirrorLifecycle {
    /// Lifecycle event handler which is invoked exactly once when the agent starts.
    ///
    /// The handler will spawn a stream which will generate random ripples and prune charges.
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<MirrorAgent>) -> impl EventHandler<MirrorAgent> {
        let rng = self.rng.clone();
        let stream = unfold((rng, Duration::default()), move |(rng, delay)| async move {
            sleep(delay).await;

            let handler =
                generate_ripple(context, rng.clone()).followed_by(cleanup_charges(context));
            let next_delay = {
                let rng = &mut *rng.lock();
                Duration::from_millis(rng.gen_range(Config::MIN_DELAY..=Config::MAX_DELAY))
            };

            Some((handler, (rng, next_delay)))
        });
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || info!(%uri, "Started agent")))
            .followed_by(context.suspend_schedule(stream.boxed()))
    }

    /// Lifecycle event handler which is invoked exactly once when a new ripple has been received by
    /// the "ripple" lane. This handler will set the state of the "ripples" lane to the received
    /// `ripple`.
    #[on_command(ripple)]
    pub fn on_ripple(
        &self,
        context: HandlerContext<MirrorAgent>,
        ripple: &Ripple,
    ) -> impl EventHandler<MirrorAgent> {
        trace!(?ripple, "New ripple");
        context.set_value(MirrorAgent::RIPPLES, ripple.clone())
    }

    /// Lifecycle event handler which is invoked exactly once when a new charge action has been
    /// received by the "charge" lane.
    #[on_command(charge)]
    pub fn on_charge_action(
        &self,
        context: HandlerContext<MirrorAgent>,
        action: &ChargeAction,
    ) -> impl EventHandler<MirrorAgent> {
        trace!(?action, "New charge action");
        match action.clone() {
            ChargeAction::Create {
                id,
                x,
                y,
                color,
                radius,
            } => {
                // Create a new charge
                context
                    .update(
                        MirrorAgent::CHARGES,
                        id.clone(),
                        Charge {
                            id,
                            x,
                            y,
                            color,
                            radius,
                            created: Timestamp::now(),
                            updated: Timestamp::now(),
                        },
                    )
                    .boxed()
            }
            ChargeAction::Move {
                id,
                x,
                y,
                color,
                radius,
            } => {
                // A 'charge' has moved, so we need to update its current state.
                context
                    .get_entry(MirrorAgent::CHARGES, id.clone())
                    .and_then(move |entry: Option<Charge>| {
                        let handler = match entry {
                            Some(charge) => {
                                let Charge { id, created, .. } = charge;
                                let updated = Charge {
                                    id: id.clone(),
                                    x,
                                    y,
                                    color,
                                    radius,
                                    created,
                                    // Update the timestamp to ensure that this charge isn't pruned
                                    // by the 'cleanup_charges` function.
                                    updated: Timestamp::now(),
                                };
                                Some(context.update(MirrorAgent::CHARGES, id, updated))
                            }
                            None => None,
                        };
                        handler.discard()
                    })
                    .boxed()
            }
            ChargeAction::Remove { id } => {
                // A charge has ended so remove the corresponding entry.
                context.remove(MirrorAgent::CHARGES, id).boxed()
            }
        }
    }
}

/// A model of a ripple.
#[derive(Debug, Form, Clone)]
pub struct Ripple {
    /// The ID of the user which is generated the ripple.
    id: Id,
    /// The ripple's X position.
    x: f64,
    /// The ripple's Y position.
    y: f64,
    /// Phases are the subsequent ripples that are generated by a ripple.
    phases: Vec<f64>,
    /// The color of the charge.
    color: String,
}

impl Default for Ripple {
    fn default() -> Self {
        Ripple::random(
            &mut StdRng::from_entropy(),
            Config::MIN_PHASES,
            Config::MAX_PHASES,
        )
    }
}

impl Ripple {
    /// Generate a random ripple.
    ///
    /// # Arguments:
    /// * `rng` - The random number generator to use.
    /// * `min_phases` - The minimum number of phases that this ripple will have.
    /// * `rng` - The maximum number of phases that this ripple will have.
    fn random(rng: &mut StdRng, min_phases: usize, max_phases: usize) -> Ripple {
        let phases = (0..rng.gen_range(min_phases..max_phases))
            .map(|_| rng.gen::<f64>())
            .collect();
        Ripple {
            id: Id::System(TAG.to_string()),
            x: rng.gen::<f64>(),
            y: rng.gen::<f64>(),
            phases,
            color: Color::select_random(rng).to_string(),
        }
    }
}

/// A color which is associated with a ripple or charge.
#[derive(Debug, Copy, Clone)]
pub enum Color {
    /// #80dc1a.
    Green,
    /// #c200fa.
    Magenta,
    /// #56dbb6.
    Cyan,
}

impl Display for Color {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Color::Green => write!(f, "#80dc1a"),
            Color::Magenta => write!(f, "#c200fa"),
            Color::Cyan => write!(f, "#56dbb6"),
        }
    }
}

impl Color {
    /// Returns a random color generated using `rng`.
    fn select_random(rng: &mut StdRng) -> Color {
        [Color::Green, Color::Magenta, Color::Cyan]
            .choose(rng)
            .copied()
            .expect("Slice was not empty")
    }
}

/// Returns an event handler which will generate a random ripple and set the state of the "ripples"
/// lane to it.
fn generate_ripple(
    context: HandlerContext<MirrorAgent>,
    rng: Rand,
) -> impl EventHandler<MirrorAgent> {
    context
        .get_value(MirrorAgent::MODE)
        .and_then(move |mode: Config| {
            let rng = &mut *rng.lock();
            let ripple = Ripple::random(rng, mode.min_phases, mode.max_phases);
            context.set_value(MirrorAgent::RIPPLES, ripple)
        })
}

/// Returns an event handler which will clean up any old charges which were not removed by the UI
/// automatically. This may happen if the user's browser suddenly closes and a `ChargeAction::Remove`
/// was not sent.
fn cleanup_charges(context: HandlerContext<MirrorAgent>) -> impl EventHandler<MirrorAgent> {
    context
        .get_map(MirrorAgent::CHARGES)
        .and_then(move |charges: HashMap<Id, Charge>| {
            let now = Utc::now().time();
            let to_remove = charges
                .values()
                .filter_map(move |charge| {
                    let diff = now - charge.updated.as_ref().time();
                    if diff.num_minutes() < 1 {
                        None
                    } else {
                        trace!("Removing charge: {:?}", charge.id);
                        Some(context.remove(MirrorAgent::CHARGES, charge.id.clone()))
                    }
                })
                .collect::<Vec<_>>();
            Sequentially::new(to_remove)
        })
}
