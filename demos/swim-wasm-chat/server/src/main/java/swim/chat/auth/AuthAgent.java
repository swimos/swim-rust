package swim.chat.auth;

import swim.api.SwimLane;
import swim.api.agent.AbstractAgent;
import swim.api.lane.CommandLane;
import swim.api.lane.MapLane;

public class AuthAgent extends AbstractAgent {

  @SwimLane("users")
  MapLane<String, User> users = mapLane();

  @SwimLane("addUser")
  CommandLane<User> addUser = this.<User>commandLane()
      .onCommand(u -> users.put(u.email(), u));

  @SwimLane("removeUser")
  CommandLane<User> removeUser = this.<User>commandLane()
    .onCommand(u -> users.remove(u.email()));
}
