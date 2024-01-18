use swim_wasm_guest::prelude::agent::{AgentSpecBuilder, SwimAgent};
use swim_wasm_guest::prelude::ir::AgentSpec;
use swim_wasm_guest::prelude::lanes::value::ValueLane;
use swim_wasm_guest::prelude::router::EventRouter;
use swim_wasm_guest::prelude::wasm_agent;

#[wasm_agent]
pub struct MyAgent {
    lane: ValueLane<i32>,
    twice: ValueLane<i32>,
}

impl SwimAgent for MyAgent {
    fn new() -> (Self, AgentSpec, impl EventRouter<Self>) {
        let mut spec = AgentSpecBuilder::new("test");

        let (lane, lane_router) = ValueLane::<i32>::builder(&mut spec, "lane", true)
            .on_update_then::<MyAgent, _, _>(
                |agent| &mut agent.lane,
                |ctx, _old_value, new_value| ctx.set_value(|agent| &mut agent.twice, new_value * 2),
            );

        let (twice, twice_router) = ValueLane::<i32>::builder(&mut spec, "twice", true)
            .on_update::<MyAgent, _>(|agent| &mut agent.twice, |_old_value, _new_value| {});

        let agent = MyAgent { lane, twice };

        (agent, spec.build(), lane_router.or(twice_router))
    }
}
