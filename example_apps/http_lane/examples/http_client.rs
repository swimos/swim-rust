use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let lane = "http://127.0.0.1:8080/example/1?lane=http_lane";

    let client = reqwest::Client::new();
    client.put(lane).body("13").send().await?;

    let response = client.get(lane).send().await?;
    let response_bytes = response.bytes().await?;
    println!("{:?}", response_bytes);

    Ok(())
}
