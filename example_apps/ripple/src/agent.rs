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
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::stream::unfold;
use futures::StreamExt;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time::sleep;

use swimos::agent::agent_lifecycle::HandlerContext;
use swimos::agent::event_handler::{EventHandler, HandlerActionExt, Sequentially};
use swimos::agent::lanes::{CommandLane, MapLane, ValueLane};
use swimos::agent::{lifecycle, projections, AgentLaneModel};
use swimos::model::{Text, Timestamp, ValueKind};
use swimos_form::read::{
    ExpectedEvent, ReadError, ReadEvent, Recognizer, RecognizerReadable, SimpleAttrBody,
    SimpleRecBody,
};
use swimos_form::write::{PrimitiveWriter, StructuralWritable, StructuralWriter};
use swimos_form::Form;

const TAG: &str = "swim0";
type Id = String;
type Rand = Arc<Mutex<StdRng>>;

#[derive(Clone, Debug, Form)]
#[form(fields_convention = "camel")]
pub struct Config {
    min_ripples: usize,
    max_ripples: usize,
    ripple_duration: usize,
    ripple_spread: usize,
}

impl Config {
    pub const MIN_RIPPLES: usize = 2;
    pub const MAX_RIPPLES: usize = 5;
}

impl Default for Config {
    fn default() -> Self {
        Config {
            min_ripples: Self::MIN_RIPPLES,
            max_ripples: Self::MAX_RIPPLES,
            ripple_duration: 5000,
            ripple_spread: 3000,
        }
    }
}

#[derive(AgentLaneModel)]
#[projections]
pub struct MirrorAgent {
    mode: ValueLane<Config>,
    ripple: CommandLane<Ripple>,
    ripples: ValueLane<Ripple>,
    charge: CommandLane<ChargeAction>,
    charges: MapLane<Id, Charge>,
}

#[derive(Debug, Form, Clone)]
pub struct Charge {
    id: Id,
    x: f64,
    y: f64,
    color: Color,
    #[form(name = "r")]
    radius: i32,
    #[form(name = "t0")]
    created: Timestamp,
    #[form(name = "t")]
    updated: Timestamp,
}

#[derive(Debug, Form, Clone)]
pub enum ChargeAction {
    Hold {
        id: Id,
        x: f64,
        y: f64,
        color: Color,
        #[form(name = "r")]
        radius: i32,
    },
    Move {
        id: Id,
        x: f64,
        y: f64,
        color: Color,
        #[form(name = "r")]
        radius: i32,
    },
    // todo: weird name?
    Up {
        id: Id,
    },
}

#[derive(Clone)]
pub struct MirrorLifecycle {
    rng: Arc<Mutex<StdRng>>,
}

impl MirrorLifecycle {
    pub fn new(rng: StdRng) -> MirrorLifecycle {
        MirrorLifecycle {
            rng: Arc::new(Mutex::new(rng)),
        }
    }
}

const MIN_DELAY: u64 = 500;
const MAX_DELAY: u64 = 2000;

#[lifecycle(MirrorAgent)]
impl MirrorLifecycle {
    #[on_start]
    pub fn on_start(&self, context: HandlerContext<MirrorAgent>) -> impl EventHandler<MirrorAgent> {
        println!("Started agent");
        let rng = self.rng.clone();
        let stream = unfold((rng, Duration::default()), move |(rng, delay)| async move {
            sleep(delay).await;

            let handler =
                generate_ripple(context, rng.clone()).followed_by(cleanup_ripples(context));
            let next_delay = {
                let rng = &mut *rng.lock();
                Duration::from_millis(rng.gen_range(MIN_DELAY..=MAX_DELAY))
            };

            Some((handler, (rng, next_delay)))
        });
        context.suspend_schedule(stream.boxed())
    }

    #[on_event(ripples)]
    pub fn on_event(
        &self,
        context: HandlerContext<MirrorAgent>,
        ripple: &Ripple,
    ) -> impl EventHandler<MirrorAgent> {
        let dbg = format!("{ripple:?}");
        context.effect(move || {
            println!("Latest ripple: {dbg}");
        })
    }

    #[on_command(ripple)]
    pub fn on_ripple(
        &self,
        context: HandlerContext<MirrorAgent>,
        ripple: &Ripple,
    ) -> impl EventHandler<MirrorAgent> {
        context.set_value(MirrorAgent::RIPPLES, ripple.clone())
    }

    #[on_command(charge)]
    pub fn on_charge_action(
        &self,
        context: HandlerContext<MirrorAgent>,
        action: &ChargeAction,
    ) -> impl EventHandler<MirrorAgent> {
        match action.clone() {
            ChargeAction::Hold {
                id,
                x,
                y,
                color,
                radius,
            } => context
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
                .boxed(),
            ChargeAction::Move {
                id,
                x,
                y,
                color,
                radius,
            } => context
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
                                updated: Timestamp::now(),
                            };
                            Some(context.update(MirrorAgent::CHARGES, id, updated))
                        }
                        None => None,
                    };
                    handler.discard()
                })
                .boxed(),
            ChargeAction::Up { id } => context.remove(MirrorAgent::CHARGES, id).boxed(),
        }
    }
}

#[derive(Debug, Form, Clone)]
pub struct Ripple {
    id: Id,
    x: f64,
    y: f64,
    phases: Option<Vec<f64>>,
    color: Color,
}

impl Default for Ripple {
    fn default() -> Self {
        Ripple::from_color(
            &mut StdRng::from_entropy(),
            Color::Green,
            Config::MIN_RIPPLES,
            Config::MAX_RIPPLES,
        )
    }
}

impl Ripple {
    fn from_color(
        rng: &mut StdRng,
        color: Color,
        min_ripples: usize,
        max_ripples: usize,
    ) -> Ripple {
        let phases = (0..rng.gen_range(min_ripples..max_ripples))
            .map(|_| rng.gen::<f64>())
            .collect();
        Ripple {
            id: TAG.to_string(),
            x: rng.gen::<f64>(),
            y: rng.gen::<f64>(),
            phases: Some(phases),
            color,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Color {
    Green,
    Magenta,
    Cyan,
}

impl RecognizerReadable for Color {
    type Rec = ColorRecognizer;
    type AttrRec = SimpleAttrBody<Self::Rec>;
    type BodyRec = SimpleRecBody<Self::Rec>;

    fn make_recognizer() -> Self::Rec {
        ColorRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(ColorRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(ColorRecognizer)
    }
}

impl StructuralWritable for Color {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_text(self.as_ref())
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_text(self.as_ref())
    }

    fn num_attributes(&self) -> usize {
        0
    }
}

#[doc(hidden)]
pub struct ColorRecognizer;

impl Recognizer for ColorRecognizer {
    type Target = Color;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::TextValue(txt) => {
                Some(
                    Color::try_from(txt.as_ref()).map_err(|_| ReadError::Malformatted {
                        text: txt.into(),
                        message: Text::new("Not a valid color."),
                    }),
                )
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl Display for Color {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl AsRef<str> for Color {
    fn as_ref(&self) -> &str {
        match self {
            Color::Green => "#00a6ed",
            Color::Magenta => "#c200fa",
            Color::Cyan => "#56dbb6",
        }
    }
}

pub struct ColorParseErr;

impl TryFrom<&str> for Color {
    type Error = ColorParseErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "#00a6ed" => Ok(Color::Green),
            "#c200fa" => Ok(Color::Magenta),
            "#56dbb6" => Ok(Color::Cyan),
            _ => Err(ColorParseErr),
        }
    }
}

fn generate_ripple(
    context: HandlerContext<MirrorAgent>,
    rng: Rand,
) -> impl EventHandler<MirrorAgent> {
    context
        .get_value(MirrorAgent::MODE)
        .and_then(move |mode: Config| {
            let mut rng = &mut *rng.lock();
            let ripple =
                Ripple::from_color(&mut rng, Color::Green, mode.min_ripples, mode.max_ripples);
            context.set_value(MirrorAgent::RIPPLES, ripple)
        })
}

fn cleanup_ripples(context: HandlerContext<MirrorAgent>) -> impl EventHandler<MirrorAgent> {
    context
        .get_map(MirrorAgent::CHARGES)
        .and_then(move |charges: HashMap<Id, Charge>| {
            let now = Utc::now().time();
            let to_remove = charges
                .values()
                .filter_map(move |charge| {
                    //
                    let diff = now - charge.updated.as_ref().time();
                    if diff.num_hours() < 1 {
                        None
                    } else {
                        Some(context.remove(MirrorAgent::CHARGES, charge.id.clone()))
                    }
                })
                .collect::<Vec<_>>();
            Sequentially::new(to_remove)
        })
}
