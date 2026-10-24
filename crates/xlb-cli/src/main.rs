use clap::{Parser, Subcommand};

mod config;
mod fetch_cmd;
mod gui;
mod inspect;
mod metrics_sink;
mod pin_cmd;
mod seed_cmd;
mod serve;
mod socket;

#[derive(Parser)]
#[command(
    name = "xlb",
    about = "xlb — serve a node, inspect state, fetch blobs, view live TUI"
)]
struct Cli {
    /// Path to the Unix domain socket used by the running node.
    #[arg(short, long, global = true, default_value_t = default_socket())]
    socket: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a long-lived xlb node daemon (reads xlb-node.json).
    Serve {
        /// Path to the node config file.
        #[arg(short, long, default_value = "xlb-node.json")]
        config: String,
    },
    /// Print current node identity, classes, and governor state (one-shot).
    Inspect,
    /// Fetch a single blob by class name and BLAKE3 hash (one-shot).
    Fetch {
        /// Asset class name (must be registered in the running node).
        class: String,
        /// BLAKE3 hash of the blob to fetch (hex).
        hash: String,
        /// Save fetched bytes to this file path (optional).
        #[arg(short, long)]
        out: Option<String>,
    },
    /// Open the live TUI dashboard (connects to a running node).
    Gui,
    /// Publish a local file to a class's CDN origin (R2) at its
    /// content-addressed key. Standalone — does not need a running node.
    ///
    /// Credentials come from the environment (XLB_R2_* / AWS_* / CF_R2_*);
    /// see `xlb::R2Target::from_env`.
    Seed {
        /// Asset class name (must exist in the node config, with a cdn_fallback).
        #[arg(short, long)]
        class: String,
        /// Path to the file to seed.
        file: String,
        /// Path to the node config file (for the class's cdn_fallback template).
        #[arg(long, default_value = "xlb-node.json")]
        config: String,
        /// Append a JSONL record of the seed to this manifest file.
        #[arg(long)]
        manifest: Option<String>,
        /// Emit a single machine-readable JSON line instead of human text.
        #[arg(long)]
        json: bool,
    },
    /// Pinned trees on a running node (R1003): named sets of blobs that
    /// survive restart and are never evicted.
    Pin {
        #[command(subcommand)]
        cmd: PinCmd,
    },
}

#[derive(Subcommand)]
enum PinCmd {
    /// Declare (or replace) a tree, seed it from local dirs, publish it.
    ///
    /// `--from DIR` alone pins every `<blake3-hex>` file in DIR (e.g. a
    /// pack's `assets/`). `--manifest FILE` pins the hashes it lists, seeding
    /// whichever of them the `--from` dirs hold.
    Add {
        class: String,
        name: String,
        /// Directory of `<blake3-hex>` files; repeatable.
        #[arg(long = "from")]
        from: Vec<String>,
        /// Hash list: JSON array, `{"hashes": [...]}`, or one hex per line.
        #[arg(long)]
        manifest: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// List a class's pinned trees and how much of each the node holds.
    List {
        class: String,
        #[arg(long)]
        json: bool,
    },
    /// Remove a tree; its blobs rejoin the LRU.
    Rm { class: String, name: String },
}

fn default_socket() -> String {
    std::env::var("XDG_RUNTIME_DIR")
        .map(|d| format!("{d}/xlb-node.sock"))
        .unwrap_or_else(|_| "/tmp/xlb-node.sock".to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("warn".parse().unwrap()),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Serve { config } => serve::run(&config, &cli.socket).await,
        Cmd::Inspect => inspect::run(&cli.socket).await,
        Cmd::Fetch { class, hash, out } => {
            fetch_cmd::run(&class, &hash, out.as_deref(), &cli.socket).await
        }
        Cmd::Gui => gui::run(&cli.socket).await,
        Cmd::Seed {
            class,
            file,
            config,
            manifest,
            json,
        } => seed_cmd::run(&class, &file, &config, manifest.as_deref(), json).await,
        Cmd::Pin { cmd } => match cmd {
            PinCmd::Add {
                class,
                name,
                from,
                manifest,
                json,
            } => pin_cmd::add(&cli.socket, &class, &name, &from, manifest.as_deref(), json).await,
            PinCmd::List { class, json } => pin_cmd::list(&cli.socket, &class, json).await,
            PinCmd::Rm { class, name } => pin_cmd::rm(&cli.socket, &class, &name).await,
        },
    }
}
