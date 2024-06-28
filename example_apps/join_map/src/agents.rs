use swimos::{
    agent::agent_lifecycle::HandlerContext,
    agent::event_handler::{EventHandler, HandlerActionExt},
    agent::lanes::{JoinMapLane, MapLane},
    agent::projections,
    agent::{lifecycle, AgentLaneModel},
};

#[derive(AgentLaneModel)]
#[projections]
pub struct StreetStatisticsAgent {
    state: MapLane<String, u64>,
}

#[derive(Clone)]
pub struct StreetStatisticsLifecycle;

#[lifecycle(StreetStatisticsAgent)]
impl StreetStatisticsLifecycle {}

#[derive(AgentLaneModel)]
#[projections]
pub struct AggregatedStatisticsAgent {
    streets: JoinMapLane<String, String, u64>,
}

#[derive(Clone)]
pub struct AggregatedLifecycle;

#[lifecycle(AggregatedStatisticsAgent)]
impl AggregatedLifecycle {
    #[on_start]
    pub fn on_start(
        &self,
        context: HandlerContext<AggregatedStatisticsAgent>,
    ) -> impl EventHandler<AggregatedStatisticsAgent> {
        println!("Start agg");

        let california_downlink = context.add_map_downlink(
            AggregatedStatisticsAgent::STREETS,
            "california".to_string(),
            None,
            "/state/california",
            "state",
        );
        let texas_downlink = context.add_map_downlink(
            AggregatedStatisticsAgent::STREETS,
            "texas".to_string(),
            None,
            "/state/texas",
            "state",
        );
        let florida_downlink = context.add_map_downlink(
            AggregatedStatisticsAgent::STREETS,
            "florida".to_string(),
            None,
            "/state/florida",
            "state",
        );

        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Starting agent at: {}", uri)))
            .followed_by(california_downlink)
            .followed_by(texas_downlink)
            .followed_by(florida_downlink)
    }

    #[on_stop]
    pub fn on_stop(
        &self,
        context: HandlerContext<AggregatedStatisticsAgent>,
    ) -> impl EventHandler<AggregatedStatisticsAgent> {
        context
            .get_agent_uri()
            .and_then(move |uri| context.effect(move || println!("Stopping agent at: {}", uri)))
    }
}
