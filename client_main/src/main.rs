use swim::interface::SwimClient;

#[swim::client]
async fn main() {
    println!("Hello from Swim client!");

    let mut client = SwimClient::new();
    let r = client
        .run_session(|mut ctx| async move {
            println!("Running session");

            ctx.spawn(async {
                println!("Running spawned task");
            });
        })
        .await;

    println!("Result: {:?}", r);
}
