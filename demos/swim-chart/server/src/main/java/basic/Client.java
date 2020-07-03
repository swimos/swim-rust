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

package basic;

import swim.api.downlink.ValueDownlink;
import swim.client.ClientRuntime;
import swim.structure.Num;
import swim.structure.Value;

class CustomClient {
  public static void main(String[] args) throws InterruptedException {
    ClientRuntime swimClient = new ClientRuntime();
    swimClient.start();
    final String hostUri = "warp://localhost:9001";
    final String nodeUri = "/unit/foo";

    swimClient.command(hostUri, nodeUri, "WAKEUP", Value.absent());

    final ValueDownlink<Value> link = swimClient.downlinkValue()
        .hostUri(hostUri).nodeUri(nodeUri).laneUri("info")
        .open();

    link.set(Num.from(1000));

    Thread.sleep(2000);
    swimClient.stop();
  }
}