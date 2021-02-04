// Copyright 2015-2020 SWIM.AI inc.
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

use swim_common::form::Form;

#[derive(Default, Form, Clone, PartialEq, Debug)]
pub struct NodeProfile;

#[cfg(test)]
mod tests {
    use crate::agent::meta::metric::node::NodeProfile;
    use crate::agent::meta::metric::sender::TransformedSender;
    use crate::agent::meta::metric::ObserverEvent;
    use futures::FutureExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_node_surjection() {
        let (tx, mut rx) = mpsc::channel(1);
        let sender = TransformedSender::new(ObserverEvent::Node, tx);
        let profile = NodeProfile::default();

        assert!(sender.try_send(profile.clone()).is_ok());
        assert_eq!(
            rx.recv().now_or_never().unwrap().unwrap(),
            ObserverEvent::Node(profile)
        );
    }
}
