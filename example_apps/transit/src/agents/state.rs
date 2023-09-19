use swim::agent::{lanes::{ValueLane, JoinValueLane, MapLane, CommandLane}, AgentLaneModel, projections};

use crate::model::{vehicle::Vehicle, agency::Agency};

// Copyright 2015-2023 Swim Inc.
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
/*
@SwimLane("count")
  public ValueLane<Value> count;
  @SwimLane("agencyCount")
  public MapLane<Agency, Integer> agencyCount;

  @SwimLane("joinAgencyCount")
  public JoinValueLane<Agency, Integer> joinAgencyCount = this.<Agency, Integer>joinValueLane()
      .didUpdate(this::updateCounts);

  @SwimTransient
  @SwimLane("vehicles")
  public MapLane<String, Vehicle> vehicles;

  @SwimLane("joinAgencyVehicles")
  public JoinMapLane<Agency, String, Vehicle> joinAgencyVehicles = this.<Agency, String, Vehicle>joinMapLane()
      .didUpdate((String key, Vehicle newEntry, Vehicle oldEntry) -> vehicles.put(key, newEntry))
      .didRemove((String key, Vehicle vehicle) -> vehicles.remove(key));

  @SwimLane("speed")
  public ValueLane<Float> speed;

  @SwimTransient
  @SwimLane("agencySpeed")
  public MapLane<Agency, Float> agencySpeed;

  @SwimLane("joinStateSpeed")
  public JoinValueLane<Agency, Float> joinAgencySpeed = this.<Agency, Float>joinValueLane()
      .didUpdate(this::updateSpeeds);

  @SwimLane("addAgency")
  public CommandLane<Agency> agencyAdd = this.<Agency>commandLane().onCommand((Agency agency) -> {
    joinAgencyCount.downlink(agency).nodeUri(agency.getUri()).laneUri("count").open();
    joinAgencyVehicles.downlink(agency).nodeUri(agency.getUri()).laneUri("vehicles").open();
    joinAgencySpeed.downlink(agency).nodeUri(agency.getUri()).laneUri("speed").open();
    context.command("/country/" + getProp("country").stringValue(), "addAgency",
        agency.toValue().unflattened().slot("stateUri", nodeUri().toString()));
  }); */

#[derive(AgentLaneModel)]
#[projections]
#[agent(convention = "camel")]
pub struct StateAgent {
    count: ValueLane<usize>,
    join_agency_count: JoinValueLane<Agency, usize>,
    vehicles: MapLane<String, Vehicle>,
    speed: ValueLane<f64>,
    agency_speed: JoinValueLane<Agency, f64>,
    join_state_speed: JoinValueLane<Agency, f64>,
    add_agency: CommandLane<Agency>,
}