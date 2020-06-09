package swim.chat;

import swim.api.SwimLane;
import swim.api.agent.AbstractAgent;
import swim.api.lane.CommandLane;
import swim.api.lane.JoinValueLane;
import swim.api.lane.MapLane;
import swim.structure.Value;
import swim.uri.Uri;

/**
 * The Rooms WebAgent simply manages a list of 
 * rooms on the server using the roomsList MapLane.
 * Each server will only have one Rooms agent.
 */
public class RoomsAgent extends AbstractAgent {

  /**
   * {@code MapLane} to hold list of room for this server
   */
  @SwimLane("list")
  protected MapLane<Long, Uri> roomsList = this.<Long, Uri>mapLane();

  /**
   * {@code CommandLane} called by clients to create a new room on the server
   */
  @SwimLane("addRoom")
  private CommandLane<String> addRoom = this.<String>commandLane()
      .onCommand(uriString -> {
        // use timestamp for room UID so rooms get sorted by creation date
        final long newUuid = System.currentTimeMillis();
        // convert the string passed by client into a Uri
        Uri newUri = Uri.parse(uriString);
        // Add new room to the roomsList MapLane
        roomsList.put(newUuid, newUri);
      });

  /**
   * {@code CommandLane} called by clients to create a remove a room from the server
   */
  @SwimLane("removeRoom")
  private CommandLane<Uri> removeRoom = this.<Uri>commandLane()
      .onCommand(uri -> {
        roomsList.remove(uri);
      });

  /**
   * When the Rooms agent starts the first time, 
   * add the default public room to the list
   */
  @Override
  public void didStart() {
    command("/rooms", "addRoom", Uri.form().mold(Uri.parse("public")).toValue());
  }
    
}
