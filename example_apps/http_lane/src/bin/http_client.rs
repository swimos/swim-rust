use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let lane = "http://127.0.0.1:8080/example/1?lane=http_lane";

    let client = reqwest::Client::new();
    client.put(lane).body("13").send().await?;

    let response = client.get(lane).send().await?;
    let body = response.bytes().await?;

    let num = std::str::from_utf8(body.as_ref())?.parse::<i32>()?;
    assert_eq!(13, num);

    Ok(())
}
