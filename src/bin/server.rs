use clap::Parser;
use rustdis::{server, Error};

const PORT: u16 = 6379;

#[derive(Parser, Debug)]
struct Args {
    /// The port to listen on
    #[arg(short, long, default_value_t = PORT)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    server::run(args.port).await
}
