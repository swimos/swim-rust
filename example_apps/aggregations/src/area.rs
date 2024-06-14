use rand::seq::SliceRandom;

use swimos::{
    agent::agent_lifecycle::utility::HandlerContext,
    agent::event_handler::EventHandler,
    agent::lanes::{CommandLane, JoinValueLane, ValueLane},
    agent::lifecycle,
    agent::projections,
    agent::AgentLaneModel,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Area {
    A,
    B,
    C,
    D,
}

impl Area {
    pub fn random() -> Self {
        [Area::A, Area::B, Area::C, Area::D]
            .choose(&mut rand::thread_rng())
            .copied()
            .expect("Slice was not empty")
    }
}

#[derive(AgentLaneModel)]
#[projections]
pub struct AreaAgent {
    register: CommandLane<u64>,
    deregister: CommandLane<u64>,
    // car_id -> speed
    cars: JoinValueLane<u64, u64>,
    average_speed: ValueLane<f64>,
}

#[derive(Clone)]
pub struct AreaLifecycle;

#[lifecycle(AreaAgent)]
impl AreaLifecycle {
    #[on_command(register)]
    pub fn register(
        &self,
        context: HandlerContext<AreaAgent>,
        car_id: &u64,
    ) -> impl EventHandler<AreaAgent> {
        let car_id = *car_id;
        context.add_downlink(
            AreaAgent::REGISTER,
            car_id,
            None,
            car_id.to_string().as_str(),
            "speed",
        )
    }

    #[on_command(deregister)]
    pub fn deregister(
        &self,
        context: HandlerContext<AreaAgent>,
        _car_id: &u64,
    ) -> impl EventHandler<AreaAgent> {

    }
}
