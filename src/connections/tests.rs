use crate::connections::Connection;

#[test]
fn test_new_connection() {
    // Given
    let host = "ws://127.0.0.1:9001";
    let buffer_size = 5;
    // When
    let (connection, transmitter) = Connection::new(host.clone(), buffer_size.clone()).unwrap();
    // Then
    assert!(connection.url.host().is_some());
    assert_eq!(9001, connection.url.port().unwrap());
    assert_eq!("ws://127.0.0.1:9001/", connection.url.as_str());
}

#[test]
fn test_new_connection_parse_error() {
    // Given
    let host = "foo";
    let buffer_size = 5;
    // When
    let result = Connection::new(host.clone(), buffer_size.clone());
    // Then
    assert!(result.is_err())
}