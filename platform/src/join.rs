use std::{os::unix::fs::PermissionsExt, path::PathBuf, process::Stdio};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{fs, process::Command};
use uuid::Uuid;

#[derive(Serialize)]
struct JoinRequest<'a> {
    token: &'a str,
}

#[derive(Deserialize)]
struct Credentials {
    node_id: Uuid,
    credential: String,
}

fn value(config: &str, key: &str) -> Option<String> {
    config.lines().find_map(|line| {
        line.strip_prefix(key)
            .and_then(|line| line.strip_prefix('='))
            .map(str::to_owned)
    })
}

async fn systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .args(args)
        .stdin(Stdio::null())
        .status()
        .await?;
    if !status.success() {
        bail!("systemctl {} failed", args.join(" "));
    }
    Ok(())
}

pub async fn run(
    controller: String,
    token: String,
    name: Option<String>,
    config_path: PathBuf,
    data_dir: PathBuf,
) -> Result<()> {
    if !controller.starts_with("http://") && !controller.starts_with("https://") {
        bail!("controller must be an http or https URL");
    }
    let name = name.unwrap_or(fs::read_to_string("/etc/hostname").await?.trim().to_owned());
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
    {
        bail!("name may contain only letters, numbers, dots, dashes, and underscores");
    }
    let current = fs::read_to_string(&config_path).await.unwrap_or_default();
    let cpu = value(&current, "SCRAPERACK_NODE_CPU_MILLIS")
        .context("SCRAPERACK_NODE_CPU_MILLIS is missing from the node configuration")?;
    let memory = value(&current, "SCRAPERACK_NODE_MEMORY_MIB")
        .context("SCRAPERACK_NODE_MEMORY_MIB is missing from the node configuration")?;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/internal/join",
            controller.trim_end_matches('/')
        ))
        .json(&JoinRequest { token: &token })
        .send()
        .await?
        .error_for_status()
        .context("the controller rejected the join token")?
        .json::<Credentials>()
        .await?;
    let config = format!(
        "SCRAPERACK_CONTROLLER={}\nSCRAPERACK_NODE_CREDENTIAL={}\nSCRAPERACK_NODE_NAME={}\nSCRAPERACK_NODE_CPU_MILLIS={}\nSCRAPERACK_NODE_MEMORY_MIB={}",
        controller.trim_end_matches('/'),
        response.credential,
        name,
        cpu,
        memory
    );

    systemctl(&["stop", "scraperack-node"]).await?;
    fs::write(&config_path, config).await?;
    fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600)).await?;
    fs::write("/etc/scraperack/role", "node").await?;
    fs::create_dir_all(&data_dir).await?;
    fs::write(data_dir.join("node-id"), response.node_id.to_string()).await?;
    systemctl(&[
        "disable",
        "--now",
        "scraperack-controller",
        "scraperack-gui",
    ])
    .await?;
    systemctl(&["enable", "--now", "scraperack-node"]).await?;
    println!("Joined {controller} as {name}");
    Ok(())
}
