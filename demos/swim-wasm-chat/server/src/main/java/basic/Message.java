package basic;

import java.util.UUID;

public class Message {

  private String value;
  private String userName;
  private String uuid;

  public Message(String value, String userName) {
    this.value = value;
    this.userName = userName;
    this.uuid = UUID.randomUUID().toString();
  }

  @Override
  public String toString() {
    return "Message{" +
        "value='" + value + '\'' +
        ", userName='" + userName + '\'' +
        '}';
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

}
