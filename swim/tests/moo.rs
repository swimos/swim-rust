use swim::agent::AgentLaneModel;
use swim_agent::lanes::{JoinMapLane, JoinValueLane};

#[test]
fn single_join_value_lane() {
    #[derive(AgentLaneModel)]
    struct SingleJoinValueLane {
        lane: JoinValueLane<i32, i32>,
    }

    //check_agent::<SingleJoinValueLane>(vec![persistent_lane(0, "lane", WarpLaneKind::JoinValue)]);
}

#[test]
fn single_join_map_lane() {
    #[derive(AgentLaneModel)]
    struct SingleJoinMapLane {
        lane: JoinMapLane<i32, i32, i32>,
    }

    //check_agent::<SingleJoinMapLane>(vec![persistent_lane(0, "lane", WarpLaneKind::JoinMap)]);
}
