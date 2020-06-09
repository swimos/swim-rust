package swim.chat;

import swim.actor.ActorSpace;
import swim.api.SwimRoute;
import swim.api.agent.AgentRoute;
import swim.api.plane.AbstractPlane;
import swim.kernel.Kernel;
import swim.server.ServerLoader;
import swim.structure.Value;
import java.io.IOException;

/**
 * Basic swim plane. sets up routes to WebAgent and starts the server and plane
 */
public class ChatPlane extends AbstractPlane {

  /**
   * define route to the Rooms webagent to manage the chat room for this server
   */
  @SwimRoute("/rooms")
  AgentRoute<RoomsAgent> roomsAgent;

  /**
   * define route to handle dynamically created webagent for each room on the server
   */
  @SwimRoute("/room/:id")
  AgentRoute<RoomAgent> roomAgent;

  public ChatPlane() {
    // no-op
  }

  /**
   * app main method. creates swim server, swim plane and starts everything
   */
  public static void main(String[] args) {
    final Kernel kernel = ServerLoader.loadServer();
    final ActorSpace space = (ActorSpace) kernel.getSpace("chat");

    kernel.start();
    System.out.println("Running ChatPlane...");
    kernel.run();

    // send a command to the Rooms WebAgent to 
    space.command("/rooms/", "WAKE", Value.absent());
  }

}
