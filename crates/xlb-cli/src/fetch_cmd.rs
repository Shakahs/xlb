//! `xlb fetch` — ask the daemon for a blob and print where its file is.

use anyhow::Result;
use xlb_client::Client;

pub async fn run(class: &str, hash: &str, socket_path: &str) -> Result<()> {
    let mut client = Client::connect(socket_path).await?;
    let result = client
        .fetch(class, hash, |progress| match progress.total {
            Some(total) => eprintln!(
                "{:?}: {} / {total} bytes",
                progress.tier, progress.bytes_so_far
            ),
            None => eprintln!("{:?}: {} bytes", progress.tier, progress.bytes_so_far),
        })
        .await?;
    eprintln!(
        "fetched {} bytes via {:?} in {}ms",
        result.bytes, result.tier, result.elapsed_ms
    );
    println!("{}", result.path);
    Ok(())
}
