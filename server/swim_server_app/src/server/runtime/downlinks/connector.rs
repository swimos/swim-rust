// Copyright 2015-2021 Swim Inc.
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

use std::num::NonZeroUsize;

use swim_remote::AttachClient;
use swim_runtime::agent::DownlinkRequest;
use swim_utilities::trigger;
use tokio::sync::mpsc;

use crate::server::runtime::ClientRegistration;

pub enum DlTaskRequest {
    Registration(ClientRegistration),
    Local(AttachClient),
}

pub struct ServerConnector {
    dl_req_tx: mpsc::Sender<DownlinkRequest>,
    client_reg_rx: mpsc::Receiver<ClientRegistration>,
    local_rx: mpsc::Receiver<AttachClient>,
    stop_downlinks: Option<trigger::Sender>,
    downlinks_stopped: Option<trigger::Receiver>,
}

impl ServerConnector {
    pub async fn next_message(&mut self) -> Option<DlTaskRequest> {
        let ServerConnector {
            client_reg_rx,
            local_rx,
            downlinks_stopped,
            ..
        } = self;
        if let Some(stopped) = downlinks_stopped.as_mut() {
            tokio::select! {
                _ = stopped => {
                    *downlinks_stopped = None;
                    None
                }
                maybe_reg = client_reg_rx.recv() => maybe_reg.map(DlTaskRequest::Registration),
                maybe_local = local_rx.recv() => maybe_local.map(DlTaskRequest::Local),
            }
        } else {
            None
        }
    }

    pub fn dl_requests(&self) -> mpsc::Sender<DownlinkRequest> {
        self.dl_req_tx.clone()
    }

    pub fn stop(&mut self) {
        if let Some(stop) = self.stop_downlinks.take() {
            stop.trigger();
        }
    }
}

pub struct DownlinksConnector {
    dl_req_rx: mpsc::Receiver<DownlinkRequest>,
    client_reg_tx: mpsc::Sender<ClientRegistration>,
    local_tx: mpsc::Sender<AttachClient>,
    stopped: bool,
    stop_downlinks: trigger::Receiver,
    downlinks_stopped: trigger::Sender,
}

pub struct Failed;

impl DownlinksConnector {
    pub async fn next_request(&mut self) -> Option<DownlinkRequest> {
        let DownlinksConnector {
            stopped,
            stop_downlinks,
            dl_req_rx,
            ..
        } = self;
        if !*stopped {
            tokio::select! {
                _ = stop_downlinks => {
                    *stopped = true;
                    None
                }
                maybe_req = dl_req_rx.recv() => maybe_req,
            }
        } else {
            None
        }
    }

    pub fn local_handle(&self) -> mpsc::Sender<AttachClient> {
        self.local_tx.clone()
    }

    pub async fn register(&self, reg: ClientRegistration) -> Result<(), Failed> {
        self.client_reg_tx.send(reg).await.map_err(|_| Failed)
    }

    pub fn stop_handle(&self) -> trigger::Receiver {
        self.stop_downlinks.clone()
    }

    pub fn stopped(self) {
        self.downlinks_stopped.trigger();
    }
}

pub fn downlink_task_connector(
    client_request_channel_size: NonZeroUsize,
    open_downlink_channel_size: NonZeroUsize,
) -> (ServerConnector, DownlinksConnector) {
    let (dl_req_tx, dl_req_rx) = mpsc::channel(open_downlink_channel_size.get());
    let (client_reg_tx, client_reg_rx) = mpsc::channel(client_request_channel_size.get());
    let (local_tx, local_rx) = mpsc::channel(client_request_channel_size.get());
    let (stop_downlinks_tx, stop_downlinks_rx) = trigger::trigger();
    let (downlinks_stopped_tx, downlinks_stopped_rx) = trigger::trigger();

    let server_end = ServerConnector {
        dl_req_tx,
        client_reg_rx,
        local_rx,
        stop_downlinks: Some(stop_downlinks_tx),
        downlinks_stopped: Some(downlinks_stopped_rx),
    };

    let downlinks_end = DownlinksConnector {
        dl_req_rx,
        client_reg_tx,
        local_tx,
        stopped: false,
        stop_downlinks: stop_downlinks_rx,
        downlinks_stopped: downlinks_stopped_tx,
    };

    (server_end, downlinks_end)
}
