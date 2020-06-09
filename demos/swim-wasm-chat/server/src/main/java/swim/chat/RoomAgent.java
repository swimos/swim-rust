package swim.chat;

import swim.api.SwimLane;
import swim.api.agent.AbstractAgent;
import swim.api.lane.CommandLane;
import swim.api.lane.MapLane;
import swim.api.lane.ValueLane;
import swim.api.warp.WarpUplink;
import swim.collections.HashTrieMap;
import swim.uri.Uri;
import java.util.Map;

/**
 * This is a dynamically created WebAgent which handles the chat messages and users for a room. One webAgent is created
 * per room. A new WebAgent is created at the time the first user accesses the room and will persist until the server is
 * stopped. Chat rooms and messages will not persist after a server restart.
 */
public class RoomAgent extends AbstractAgent {

  // how long chat messages persist before being culled
  private static final long THRESHOLD_MILLIS = 60 * 60 * 1000;

  /**
   * Simple {@code ValueLane} to track the number of users in this room.
   */
  @SwimLane("userCount")
  protected ValueLane<Integer> userCount = this.<Integer>valueLane();

  /**
   * Simple {@code MapLane} to track the users in this room.
   */
  @SwimLane("users")
  public MapLane<String, String> users = this.<String, String>mapLane()
      .didUpdate((k, n, o) -> {
        this.userCount.set(this.users.size());
      });

  /**
   * {@code MapLane} to hold all the chat messages for this room
   */
  @SwimLane("messageList")
  protected MapLane<Long, String> messageList = this.<Long, String>mapLane()
      // listen for message list changes and purge old messages when 
      .didUpdate((index, newValue, oldValue) -> {
        purgeOldMessages();
      })
      // listen for when a client links to this lane
      // this will be used for tracking users leaving/entering room
      .didUplink(uplink -> {

        // get remote address for client which created this link
        final String userId = uplink.remoteAddress().toString();
        // extract IP address from userId and use it for the userName
        final String userName = userId.substring(1, userId.indexOf(':'));
        // listen for when each uplink changes 
        this.uplinks = this.uplinks.updated(
            userId,
            uplink
                // listen for when the uplink connects
                .onLinked((v) -> {
                  // create timestamp
                  final long now = System.currentTimeMillis();

                  // create message in message list 
                  // notifying that the user entered the room
                  this.messageList.put(now, "{\"userId\": \"0\", \"msg\": \"" + userName + " entered room\"}");

                  // add user for uplink to the users lane of this room
                  this.users.put(userName, userName);

                  // update room user count
                  this.userCount.set(this.users.size());
                })

                // listen for when uplink is closed
                .onUnlinked((v) -> {
                  // create timestamp
                  final long now = System.currentTimeMillis();

                  // create message in message list 
                  // notifying that the user left the room
                  this.messageList.put(now, "{\"userId\": \"0\", \"msg\": \"" + userName + " left room\"}");

                  // remove user from users lane
                  this.users.remove(userName);

                  // remove uplink from uplinks lane
                  this.uplinks = this.uplinks.removed(userId);

                  // update room user count
                  this.userCount.set(this.users.size());
                })
        );

      });

  /**
   * Map to hold each client uplink created to the message list lane
   */
  private HashTrieMap<String, WarpUplink> uplinks = HashTrieMap.empty();

  /**
   * {@code CommandLane} used by swim clients to post new messages to the MessageList lane for this room.
   */
  @SwimLane("postMessage")
  private CommandLane<String> postMessage = this.<String>commandLane()
      .onCommand(msgPacket -> {
        // create a timestamp for the new message
        final long now = System.currentTimeMillis();
        // add message to messageList lane
        messageList.put(now, msgPacket);
      });

  /**
   * private method used to clean out chat messages that are older then THRESHOLD_MILLIS
   */
  private void purgeOldMessages() {
    final long now = System.currentTimeMillis();
    int count = 0;

    // for each message in the message list
    for (Map.Entry<Long, String> entry : this.messageList.entrySet()) {
      // if message is older then the threshold
      if (now - entry.getKey() > THRESHOLD_MILLIS) {
        // remove the message from the list
        this.messageList.remove(entry.getKey());
      }
    }
  }

  /**
   * Remove room from the Rooms WebAgent when this room is stopped
   */
  @Override
  public void willStop() {
    // System.out.println("WILL STOP");
    command("/rooms", "removeRoom", Uri.form().mold(nodeUri()).toValue());
  }

}
