use swim_wasm_guest::prelude::agent::{AgentSpecBuilder, SwimAgent};
use swim_wasm_guest::prelude::ir::AgentSpec;
use swim_wasm_guest::prelude::lanes::value::ValueLane;
use swim_wasm_guest::prelude::router::EventRouter;
use swim_wasm_guest::prelude::wasm_agent;

#[wasm_agent]
pub struct MyAgent {
    lane: ValueLane<i32>,
}

impl SwimAgent for MyAgent {
    fn new() -> (Self, AgentSpec, impl EventRouter<Self>) {
        let mut spec = AgentSpecBuilder::new("MyAgent");

        let (lane, lane_router) = ValueLane::<i32>::builder(&mut spec, "lane", true)
            .on_update::<MyAgent, _>(
                |agent| &mut agent.lane,
                |_old_value, new_value| println!("New value: {new_value}"),
            );

        let agent = MyAgent { lane };

        (agent, spec.build(), lane_router)
    }
}
