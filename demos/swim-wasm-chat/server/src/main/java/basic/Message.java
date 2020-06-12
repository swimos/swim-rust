package basic;

public class Message {

  private String value;
  private String userName;
  private String userUuid;

  public Message(String value, String userName, String userUuid) {
    this.value = value;
    this.userName = userName;
    this.userUuid = userUuid;
  }

  public String getUserUuid() {
    return userUuid;
  }

  public void setUserUuid(String userUuid) {
    this.userUuid = userUuid;
  }

  @Override
  public String toString() {
    return "Message{" +
        "value='" + value + '\'' +
        ", userName='" + userName + '\'' +
        ", userUuid='" + userUuid + '\'' +
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
