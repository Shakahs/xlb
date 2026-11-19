//! `xlb` — run a node daemon, or talk to one over its control socket.

mod config;
mod fetch_cmd;
mod inspect;
mod metrics_sink;
mod pin_cmd;
mod serve;
mod server;

use anyhow::{bail, Context, Result};

const USAGE: &str = "\
xlb — serve a node, inspect it, fetch blobs, manage peers and pinned trees

usage: xlb [--socket PATH] <command>

commands:
  serve [--config FILE]                 run the node daemon (default xlb-node.json)
  inspect                               node ID, UDP addresses, cache directory
  fetch CLASS HASH                      fetch a blob; prints the cached file path
  set-peers [--class CLASS] [PEER...]   replace the peer set; PEER is NODE_ID@ADDR[,ADDR...]
  pin add CLASS NAME [--from DIR]... [--manifest FILE] [--json]
  pin list CLASS [--json]
  pin rm CLASS NAME
";

/// The command line after the program name, with flags pulled out.
#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    positional: Vec<String>,
    socket: Option<String>,
    config: Option<String>,
    class: Option<String>,
    manifest: Option<String>,
    from: Vec<String>,
    json: bool,
    help: bool,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args> {
    let mut parsed = Args::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let mut value = |flag: &str| {
            args.next()
                .with_context(|| format!("{flag} needs a value"))
        };
        match arg.as_str() {
            "-s" | "--socket" => parsed.socket = Some(value("--socket")?),
            "-c" | "--config" => parsed.config = Some(value("--config")?),
            "--class" => parsed.class = Some(value("--class")?),
            "--manifest" => parsed.manifest = Some(value("--manifest")?),
            "--from" => parsed.from.push(value("--from")?),
            "--json" => parsed.json = true,
            "-h" | "--help" => parsed.help = true,
            flag if flag.starts_with('-') => bail!("unknown flag {flag}\n\n{USAGE}"),
            _ => parsed.positional.push(arg),
        }
    }
    Ok(parsed)
}

fn default_socket() -> String {
    std::env::var("XDG_RUNTIME_DIR")
        .map(|d| format!("{d}/xlb-node.sock"))
        .unwrap_or_else(|_| "/tmp/xlb-node.sock".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = parse_args(std::env::args().skip(1))?;
    let words: Vec<&str> = args.positional.iter().map(String::as_str).collect();
    if args.help || words.is_empty() {
        print!("{USAGE}");
        return Ok(());
    }
    if words == ["serve"] {
        let config = args.config.as_deref().unwrap_or("xlb-node.json");
        return serve::run(config, args.socket.as_deref()).await;
    }
    let socket = args.socket.clone().unwrap_or_else(default_socket);

    match words.as_slice() {
        ["inspect"] => inspect::run(&socket).await,
        ["fetch", class, hash] => fetch_cmd::run(class, hash, &socket).await,
        ["set-peers", peers @ ..] => {
            inspect::set_peers(&socket, args.class.as_deref(), peers).await
        }
        ["pin", "add", class, name] => {
            pin_cmd::add(
                &socket,
                class,
                name,
                &args.from,
                args.manifest.as_deref(),
                args.json,
            )
            .await
        }
        ["pin", "list", class] => pin_cmd::list(&socket, class, args.json).await,
        ["pin", "rm", class, name] => pin_cmd::rm(&socket, class, name).await,
        _ => bail!("unrecognised command: {}\n\n{USAGE}", words.join(" ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Args {
        parse_args(line.split_whitespace().map(String::from)).unwrap()
    }

    #[test]
    fn flags_are_accepted_before_and_after_the_command() {
        let args = parse("--socket /s.sock pin add models keys --from /a --from /b --json");
        assert_eq!(args.positional, ["pin", "add", "models", "keys"]);
        assert_eq!(args.socket.as_deref(), Some("/s.sock"));
        assert_eq!(args.from, ["/a", "/b"]);
        assert!(args.json);
    }

    #[test]
    fn a_flag_without_its_value_and_an_unknown_flag_are_errors() {
        assert!(parse_args(["serve".to_string(), "--config".to_string()]).is_err());
        assert!(parse_args(["--bogus".to_string()]).is_err());
    }
}
