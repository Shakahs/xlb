//! `xlb pin {add,list,rm}` — pinned trees on a running node (R1003).

use anyhow::{bail, Context, Result};

use crate::socket::{
    client::Client,
    protocol::{Command, Response, TreeInfo},
};

/// Read a manifest of blake3 hashes: a JSON array of hex strings, a JSON
/// object with a `hashes` array, or plain text with one or more hex hashes
/// per line (`#` starts a comment). Validated by the node, not here.
pub fn read_manifest(path: &str) -> Result<Vec<String>> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
        let arr = match &v {
            serde_json::Value::Array(a) => Some(a),
            serde_json::Value::Object(o) => o.get("hashes").and_then(|h| h.as_array()),
            _ => None,
        };
        let Some(arr) = arr else {
            bail!("{path}: JSON manifest must be an array of hashes or {{\"hashes\": [...]}}");
        };
        return arr
            .iter()
            .map(|h| {
                h.as_str()
                    .map(str::to_owned)
                    .with_context(|| format!("{path}: non-string hash {h}"))
            })
            .collect();
    }
    Ok(raw
        .lines()
        .map(|l| l.split('#').next().unwrap_or(""))
        .flat_map(str::split_whitespace)
        .map(str::to_owned)
        .collect())
}

/// The node reads `--from` dirs itself, from its own cwd; send absolute paths.
fn absolute(dirs: &[String]) -> Result<Vec<String>> {
    dirs.iter()
        .map(|d| {
            std::fs::canonicalize(d)
                .with_context(|| format!("--from {d}"))
                .map(|p| p.display().to_string())
        })
        .collect()
}

pub async fn add(
    socket: &str,
    class: &str,
    name: &str,
    from: &[String],
    manifest: Option<&str>,
    json: bool,
) -> Result<()> {
    let hashes = match manifest {
        Some(m) => read_manifest(m)?,
        None => Vec::new(),
    };
    if hashes.is_empty() && from.is_empty() {
        bail!("give --manifest, --from, or both");
    }
    let cmd = Command::PinTree {
        class: class.into(),
        name: name.into(),
        hashes,
        from_dirs: absolute(from)?,
    };
    match Client::connect(socket).await?.roundtrip(&cmd).await? {
        Response::TreePinned { tree, seed } => {
            if json {
                println!("{}", serde_json::json!({ "tree": tree, "seed": seed }));
            } else {
                print_tree(&tree);
                println!(
                    "  seeded {} · already cached {} · missing {} · mismatched {} · published {}",
                    seed.ingested,
                    seed.already_cached,
                    seed.missing.len(),
                    seed.mismatched.len(),
                    seed.served
                );
                for p in &seed.mismatched {
                    println!("  MISMATCH {p}");
                }
            }
            Ok(())
        }
        other => fail(other),
    }
}

pub async fn list(socket: &str, class: &str, json: bool) -> Result<()> {
    let cmd = Command::ListTrees {
        class: class.into(),
    };
    match Client::connect(socket).await?.roundtrip(&cmd).await? {
        Response::Trees { trees } => {
            if json {
                println!("{}", serde_json::to_string(&trees)?);
            } else if trees.is_empty() {
                println!("no pinned trees on {class}");
            } else {
                trees.iter().for_each(print_tree);
            }
            Ok(())
        }
        other => fail(other),
    }
}

pub async fn rm(socket: &str, class: &str, name: &str) -> Result<()> {
    let cmd = Command::UnpinTree {
        class: class.into(),
        name: name.into(),
    };
    match Client::connect(socket).await?.roundtrip(&cmd).await? {
        Response::Ok => {
            println!("unpinned {name}");
            Ok(())
        }
        other => fail(other),
    }
}

fn print_tree(t: &TreeInfo) {
    println!(
        "{:<24} {}/{} present · {} bytes{}",
        t.name,
        t.present,
        t.members,
        t.present_bytes,
        if t.durable { "" } else { " · NOT durable (in-memory class)" }
    );
}

fn fail(resp: Response) -> Result<()> {
    match resp {
        Response::Error { message } => bail!("{message}"),
        other => bail!("unexpected response: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(body: &str) -> Vec<String> {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), body).unwrap();
        read_manifest(f.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn manifests_read_in_all_three_shapes() {
        assert_eq!(manifest(r#"["aa","bb"]"#), ["aa", "bb"]);
        assert_eq!(manifest(r#"{"hashes":["aa"],"name":"x"}"#), ["aa"]);
        assert_eq!(manifest("aa  # first\n\n# note\nbb cc\n"), ["aa", "bb", "cc"]);
    }
}
