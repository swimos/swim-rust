#[swim::client]
async fn main() {
    println!("Hello from Swim client!");

    tokio::spawn(async {
        println!("Spawned task");
    });
}
