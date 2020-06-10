// Copyright 2015-2019 SWIM.AI inc.
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

import swim.api.SwimLane;
import swim.api.agent.AbstractAgent;
import swim.api.lane.CommandLane;
import swim.api.lane.MapLane;
import swim.concurrent.TimerRef;
import java.time.ZonedDateTime;

public class ChatAgent extends AbstractAgent {

  private TimerRef timer;

  @SwimLane("chats")
  MapLane<Long, String> chats = this.<Long, String>mapLane().didUpdate((k, n, o) -> {
    logMessage("`chats` set {" + k + "} to {" + n + "} from {" + o + "}");
  });

  @SwimLane("post")
  CommandLane<String> postMessage = this.<String>commandLane().onCommand(msg-> {
    System.out.println("On command");
    final long now = System.currentTimeMillis();
    chats.put(now, msg);
  });

  private void logMessage(Object msg) {
    System.out.println(ZonedDateTime.now() + ": " + nodeUri() + ": " + msg);
  }

  @Override
  public void didStart() {
    System.out.println(hostUri() + " " + nodeUri());

    chats.put(1L, "test");
  }

}
