package swim.chat.auth;

import swim.api.auth.Identity;
import swim.api.downlink.MapDownlink;
import swim.api.plane.PlaneContext;
import swim.api.policy.AbstractPolicy;
import swim.api.policy.PolicyDirective;
import swim.collections.HashTrieSet;
import swim.recon.Recon;
import swim.structure.Value;
import swim.warp.Envelope;

public class AuthPolicy extends AbstractPolicy {

  private final HashTrieSet<String> authTokens;
  private final MapDownlink<Value, Value> users;

  public AuthPolicy(PlaneContext ref) {
    this.authTokens = HashTrieSet.empty();
    this.users = ref.downlinkMap()
        .nodeUri("/auth")
        .laneUri("users")
        .didUpdate((k, n, o) -> {
          // TODO: called during removes as well?
          System.out.println("key: " + Recon.toString(k) + ", value: " + Recon.toString(n));
        })
        .didRemove((k, o) -> {
          System.out.println("key: " + Recon.toString(k) + ", removedValue: " + Recon.toString(o));
        })
        .open();
  }

  @Override
  protected <T> PolicyDirective<T> authorize(Envelope envelope, Identity identity) {
    if (identity != null && identity.isAuthenticated()) {
      return allow();
    }
    return forbid();
  }
}
