use swim_wasm_guest::prelude::agent::{AgentSpecBuilder, SwimAgent};
use swim_wasm_guest::prelude::ir::AgentSpec;
use swim_wasm_guest::prelude::lanes::value::ValueLane;
use swim_wasm_guest::prelude::router::EventRouter;
use swim_wasm_guest::prelude::wasm_agent;

#[wasm_agent]
pub struct MyAgent {
    id_lane: ValueLane<i32>,
    counter_lane: ValueLane<u64>,
    name_lane: ValueLane<String>,
}

impl SwimAgent for MyAgent {
    fn new() -> (Self, AgentSpec, impl EventRouter<Self>) {
        let mut spec = AgentSpecBuilder::new("MyAgent");

        let (id_lane, lane_router) = ValueLane::<i32>::builder(&mut spec, "id_lane", true)
            .on_update::<MyAgent, _>(|agent| &mut agent.id_lane, |_old_value, _new_value| {});

        let (counter_lane, counter_router) =
            ValueLane::<u64>::builder(&mut spec, "counter_lane", true).on_update::<MyAgent, _>(
                |agent| &mut agent.counter_lane,
                |_old_value, _new_value| {},
            );

        let (name_lane, name_router) =
            ValueLane::<String>::builder(&mut spec, "name_lane", true)
                .on_update::<MyAgent, _>(|agent| &mut agent.name_lane, |_old_value, _new_value| {});

        let agent = MyAgent {
            id_lane,
            counter_lane,
            name_lane,
        };

        (
            agent,
            spec.build(),
            lane_router.or(counter_router).or(name_router),
        )
    }
}
