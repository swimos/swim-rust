package basic;

import swim.api.downlink.MapDownlink;
import swim.client.ClientRuntime;
import swim.structure.Form;
import swim.structure.Item;
import swim.structure.Value;
import java.util.UUID;

class Client {

  public static void main(String[] args) throws InterruptedException {
    ClientRuntime swimClient = new ClientRuntime();
    swimClient.start();
    final String hostUri = "ws://localhost:9001";
    final String nodeUri = "/rooms/post";

    Message message = new Message("v", "un");
    Form<Message> form = Form.forClass(Message.class);
    Item item = form.mold(message);

    swimClient.command(hostUri, "rooms", "post", (Value) item);

    Thread.sleep(2000);
    swimClient.stop();
  }

}