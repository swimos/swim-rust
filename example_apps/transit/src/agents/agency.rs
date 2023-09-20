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

use std::{collections::HashMap, time::Duration};

use swim::agent::{
    agent_lifecycle::utility::HandlerContext,
    event_handler::{join, Either, EventHandler, HandlerAction, HandlerActionExt, Sequentially},
    lanes::{CommandLane, DemandLane, MapLane, ValueLane},
    lifecycle, projections, AgentLaneModel,
};
use tracing::error;

use crate::{
    buses_api::BusesApi,
    model::{
        agency::Agency,
        bounding_box::BoundingBox,
        route::Route,
        vehicle::{Vehicle, VehicleResponse},
    },
};

#[derive(AgentLaneModel)]
#[projections]
#[agent(convention = "camel")]
pub struct AgencyAgent {
    #[lane(transient)]
    vehicles: MapLane<String, Vehicle>,
    #[lane(transient)]
    vehicles_count: ValueLane<usize>,
    #[lane(transient)]
    vehicles_speed: ValueLane<f64>,
    add_vehicles: CommandLane<Vec<VehicleResponse>>,
    info: DemandLane<Agency>,
    #[lane(transient)]
    routes: MapLane<String, Route>,
    #[lane(transient)]
    bounding_box: ValueLane<BoundingBox>,
}

#[derive(Debug, Clone)]
pub struct AgencyLifecycle {
    api: BusesApi,
    agency: Agency,
    poll_delay: Duration,
}

#[lifecycle(AgencyAgent)]
impl AgencyLifecycle {
    #[on_start]
    fn init(&self, context: HandlerContext<AgencyAgent>) -> impl EventHandler<AgencyAgent> + '_ {
        let this = self.clone();
        let get_routes_and_poll = async move {
            let on_routes = this.clone().load_routes(context).await;
            let start_polling = this.start_polling(context);
            on_routes.followed_by(start_polling)
        };
        context.suspend(get_routes_and_poll)
    }

    #[on_cue(info)]
    fn get_agency_info(
        &self,
        context: HandlerContext<AgencyAgent>,
    ) -> impl HandlerAction<AgencyAgent, Completion = Agency> {
        context.value(self.agency.clone())
    }

    #[on_command(add_vehicles)]
    fn update_vehicles(
        &self,
        context: HandlerContext<AgencyAgent>,
        vehicles: &[VehicleResponse],
    ) -> impl EventHandler<AgencyAgent> + '_ {
        let responses = vehicles.to_vec();
        let stats = vehicles
            .iter()
            .fold(Statistics::default(), |s, v| s.update(v));
        join(
            context.get_map(AgencyAgent::VEHICLES),
            context.get_map(AgencyAgent::ROUTES),
        )
        .and_then(move |(vehicles, routes)| {
            let vehicle_map = get_vehicle_map(&self.agency, responses, &routes);
            let Statistics {
                mean_speed,
                bounding_box,
                ..
            } = stats;
            process_new_vehicles(context, &vehicles, vehicle_map)
                .followed_by(context.set_value(AgencyAgent::VEHICLES_SPEED, mean_speed))
                .followed_by(context.set_value(AgencyAgent::BOUNDING_BOX, bounding_box))
        })
    }
}

impl AgencyLifecycle {
    pub fn new(api: BusesApi, agency: Agency, poll_delay: Duration) -> Self {
        AgencyLifecycle {
            api,
            agency,
            poll_delay,
        }
    }

    async fn load_routes(
        self,
        context: HandlerContext<AgencyAgent>,
    ) -> impl EventHandler<AgencyAgent> + 'static {
        let AgencyLifecycle { api, agency, .. } = self;
        match api.get_routes(&agency).await {
            Ok(routes) => {
                let insert_routes = Sequentially::new(routes.into_iter().map(move |route| {
                    let tag = route.tag.clone();
                    context.update(AgencyAgent::ROUTES, tag, route)
                }));
                Either::Left(insert_routes)
            }
            Err(err) => {
                error!("Failed to load routes: {}", err);
                Either::Right(context.stop())
            }
        }
    }

    async fn poll_vehicles(
        self,
        context: HandlerContext<AgencyAgent>,
    ) -> Option<impl EventHandler<AgencyAgent> + 'static> {
        let AgencyLifecycle { api, agency, .. } = &self;
        match api.poll_vehicles(&agency).await {
            Ok(vehicles) => Some(context.command(AgencyAgent::ADD_VEHICLES, vehicles)),
            Err(err) => {
                error!("Failed to load vehicles: {}", err);
                None
            }
        }
    }

    fn start_polling(
        self,
        context: HandlerContext<AgencyAgent>,
    ) -> impl EventHandler<AgencyAgent> + 'static {
        context.suspend_repeatedly(self.poll_delay, move || {
            Some(self.clone().poll_vehicles(context))
        })
    }
}

fn get_vehicle_map(
    agency: &Agency,
    vehicles: Vec<VehicleResponse>,
    routes: &HashMap<String, Route>,
) -> HashMap<String, Vehicle> {
    let mut vehicle_map = HashMap::new();
    for response in vehicles {
        if let Some(route) = routes.get(&response.route_tag) {
            let v = agency.create_vehicle(route, response);
            vehicle_map.insert(v.id.clone(), v);
        }
    }
    vehicle_map
}

fn process_new_vehicles(
    context: HandlerContext<AgencyAgent>,
    current_vehicles: &HashMap<String, Vehicle>,
    new_vehicles: HashMap<String, Vehicle>,
) -> impl EventHandler<AgencyAgent> {
    let removals = current_vehicles
        .keys()
        .filter(|k| !new_vehicles.contains_key(*k))
        .cloned()
        .map(move |k| context.remove(AgencyAgent::VEHICLES, k))
        .collect::<Vec<_>>();

    let additions = new_vehicles
        .into_iter()
        .filter(|(k, _)| !current_vehicles.contains_key(k))
        .map(move |(k, v)| {
            let to_vehicle_agent =
                context.send_command(None, v.uri.clone(), "addVehicle".to_string(), v.clone());
            let add_vehicle = context.update(AgencyAgent::VEHICLES, k, v);
            to_vehicle_agent.followed_by(add_vehicle)
        })
        .collect::<Vec<_>>();

    Sequentially::new(removals).followed_by(Sequentially::new(additions))
}

#[derive(Default)]
struct Statistics {
    mean_speed: f64,
    n: usize,
    bounding_box: BoundingBox,
}

const MIN_LAT: f64 = -90.0;
const MAX_LAT: f64 = 90.0;
const MIN_LONG: f64 = -180.0;
const MAX_LONG: f64 = 180.0;

impl Statistics {
    fn update(mut self, vehicle: &VehicleResponse) -> Self {
        let Statistics {
            mean_speed,
            n,
            bounding_box:
                BoundingBox {
                    min_lat,
                    max_lat,
                    min_lng,
                    max_lng,
                },
        } = &mut self;

        let next_n = n.checked_add(1).expect("Number of vehicles overflowed.");
        *mean_speed = (*n as f64 * *mean_speed + vehicle.speed as f64) / (next_n as f64);
        *n = next_n;

        *min_lat = min_lat.min(vehicle.latitude).clamp(MIN_LAT, MAX_LAT);
        *max_lat = max_lat.max(vehicle.latitude).clamp(MIN_LAT, MAX_LAT);
        *min_lng = min_lng.min(vehicle.longitude).clamp(MIN_LONG, MAX_LONG);
        *max_lng = max_lng.max(vehicle.longitude).clamp(MIN_LONG, MAX_LONG);
        self
    }
}
