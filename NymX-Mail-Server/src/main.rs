mod config;
mod receive;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about = "NymX Mail Server")]
struct Cli {
    #[arg(long)]
    init: bool,

    #[arg(long, required_if_eq("init", "true"))]
    gateway: Option<String>,

    #[arg(short = 'r', long)]
    receive: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.init {
        if let Some(gateway) = cli.gateway {
            config::init_client(&gateway).await?;
        }
        return Ok(());
    }

    if cli.receive {
        receive::receive_mode(None).await?;
    } else {
        println!("Usage:");
        println!("  1. Initialize: nymx-mail-server --init --gateway <identity-key>");
        println!("  2. Run server: nymx-mail-server -r");
    }

    Ok(())
}
