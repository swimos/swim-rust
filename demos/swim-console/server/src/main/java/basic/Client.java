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