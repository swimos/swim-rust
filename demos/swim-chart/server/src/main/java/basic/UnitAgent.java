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
import swim.api.lane.MapLane;
import swim.concurrent.TimerRef;
import java.time.ZonedDateTime;
import java.util.Random;

public class UnitAgent extends AbstractAgent {

  private TimerRef timer;
  private static final int MAX_SIZE = 300;

  @SwimLane("random")
  MapLane<Long, Integer> randomMap = this.<Long, Integer>mapLane()
      .didUpdate((key, newValue, oldValue) -> {
        if (this.randomMap.size() > UnitAgent.MAX_SIZE) {
          this.randomMap.remove(this.randomMap.getIndex(0).getKey());
        }

        logMessage("`randomLane` set to {" + newValue + "} from {" + oldValue + "}");
      });

  private void logMessage(Object msg) {
    System.out.println(ZonedDateTime.now() + ": " + nodeUri() + ": " + msg);
  }

  @Override
  public void didStart() {
    super.didStart();
    Random random = new Random();

    int interval = 5;

    this.timer = setTimer(interval, () -> {
      this.timer.reschedule(interval);
      final long now = System.currentTimeMillis();

      randomMap.put(now, random.nextInt(10000));
    });
  }

}
