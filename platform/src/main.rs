mod api;
mod cluster;
mod join;
mod node;

use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "scraperack")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Controller {
        #[arg(long, default_value = "0.0.0.0:7070")]
        listen: SocketAddr,
        #[arg(long, default_value = "data/controller")]
        data_dir: PathBuf,
    },
    Node {
        #[arg(long)]
        controller: String,
        #[arg(long)]
        name: String,
        #[arg(long, default_value_t = 8000)]
        cpu_millis: u32,
        #[arg(long, default_value_t = 16384)]
        memory_mib: u32,
        #[arg(long, default_value = "data/node")]
        data_dir: PathBuf,
        #[arg(long, default_value = "/usr/local/bin/firecracker")]
        firecracker: PathBuf,
        #[arg(long, default_value = "/var/lib/scraperack-node/vmlinux")]
        kernel: PathBuf,
        #[arg(long)]
        advertise_address: Option<IpAddr>,
    },
    Join {
        #[arg(long)]
        controller: String,
        #[arg(long)]
        token: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "/etc/scraperack/node.env")]
        config: PathBuf,
        #[arg(long, default_value = "/var/lib/scraperack-node")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match Cli::parse().command {
        Command::Controller { listen, data_dir } => api::serve(listen, data_dir).await,
        Command::Node {
            controller,
            name,
            cpu_millis,
            memory_mib,
            data_dir,
            firecracker,
            kernel,
            advertise_address,
        } => {
            node::run(
                controller,
                name,
                cpu_millis,
                memory_mib,
                data_dir,
                firecracker,
                kernel,
                advertise_address,
                std::env::var("SCRAPERACK_NODE_CREDENTIAL").ok(),
            )
            .await
        }
        Command::Join {
            controller,
            token,
            name,
            config,
            data_dir,
        } => join::run(controller, token, name, config, data_dir).await,
    }
}
