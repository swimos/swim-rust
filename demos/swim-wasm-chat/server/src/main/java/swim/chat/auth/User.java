package swim.chat.auth;

import java.util.Objects;
import swim.recon.Recon;
import swim.structure.Form;

/**
 * Trivial wrapper class that stores a user's email and username.
 */
class User {

  private final String email;
  private final String username;

  public User(String email, String username) {
    this.email = email;
    this.username = username;
  }

  public String email() {
    return this.email;
  }

  public String username() {
    return this.username;
  }

  // required for reflection-based Form generation
  private User() {
    this(null, null);
  }

  private static final Form<User> FORM = Form.forClass(User.class);

  public static Form<User> form() {
    return FORM;
  }

  @Override
  public String toString() {
    return Recon.toString(form().mold(this));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    User user = (User) o;
    return Objects.equals(email, user.email) &&
        Objects.equals(username, user.username);
  }

  @Override
  public int hashCode() {
    return Objects.hash(email, username);
  }
}
