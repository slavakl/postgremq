//! Embeds the schema migrations of the pinned mq release.
//!
//! The pin is `[package.metadata.postgremq] mq` in Cargo.toml. Once that mq
//! version is released its stamp migration (`NNNNNN_release_v<version>.up.sql`)
//! exists, and only the migrations up to and including it are embedded. Before
//! then (an unreleased pin) every migration is embedded; release checks refuse
//! to publish the crate in that state. `migrations/` is a symlink to
//! `../mq/migrations`, which `cargo package` follows.

use std::error::Error;
use std::fmt::Write as _;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let manifest_dir = Path::new(&manifest_dir);
    let dir = manifest_dir.join("migrations");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=migrations");

    let pin = mq_pin(&std::fs::read_to_string(manifest_dir.join("Cargo.toml"))?)?;
    let mut migrations = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        let Some(stem) = name.strip_suffix(".up.sql") else {
            continue;
        };
        let Some((number, label)) = stem.split_once('_') else {
            continue;
        };
        if let Ok(number) = number.parse::<u64>() {
            migrations.push((number, label.to_owned(), name));
        }
    }
    migrations.sort();
    if let Some(pair) = migrations.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(format!(
            "two migrations numbered {}: {} and {}",
            pair[0].0, pair[0].2, pair[1].2
        )
        .into());
    }
    if migrations.is_empty() {
        return Err(format!("no migrations in {}", dir.display()).into());
    }
    // As mq/scripts/stamp_release.py names it.
    let stamp: String = format!("release_v{pin}")
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let released = migrations
        .iter()
        .find(|(_, label, _)| *label == stamp)
        .map(|(number, _, _)| *number);
    if let Some(last) = released {
        migrations.retain(|(number, _, _)| *number <= last);
    } else {
        // Development builds only: release checks refuse to publish this.
        println!(
            "cargo:warning=mq {pin} is not released (no {stamp} migration): embedding every migration"
        );
    }

    let mut out = String::new();
    writeln!(
        out,
        "/// The pinned mq version (Cargo.toml `[package.metadata.postgremq] mq`)."
    )?;
    writeln!(out, "#[cfg(test)]\nconst MQ_VERSION: &str = {pin:?};")?;
    writeln!(
        out,
        "/// Whether the pinned mq version is released (its stamp migration exists)."
    )?;
    writeln!(
        out,
        "#[cfg(test)]\nconst MQ_RELEASED: bool = {};",
        released.is_some()
    )?;
    writeln!(out, "/// The embedded migrations, in version order.")?;
    writeln!(out, "const MIGRATIONS: &[Migration] = &[")?;
    for (number, _, name) in &migrations {
        let path = dir.join(name);
        writeln!(
            out,
            "    Migration {{ version: {number}, sql: include_str!({:?}) }},",
            path.display().to_string()
        )?;
    }
    writeln!(out, "];")?;
    std::fs::write(
        Path::new(&std::env::var("OUT_DIR")?).join("migrations.rs"),
        out,
    )?;
    Ok(())
}

/// The `mq` value of `[package.metadata.postgremq]`. Strict on purpose: the
/// line must be exactly `mq = "<version>"`, so this and the release checks
/// (scripts/release/verify_release.py) read the same pin.
fn mq_pin(manifest: &str) -> Result<String, Box<dyn Error>> {
    let mut in_table = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_table = line == "[package.metadata.postgremq]";
        } else if in_table && line.starts_with("mq") {
            let pin = line
                .strip_prefix("mq = \"")
                .and_then(|rest| rest.strip_suffix('"'))
                .filter(|pin| {
                    !pin.is_empty()
                        && pin
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'))
                })
                .ok_or_else(|| {
                    format!(
                        "Cargo.toml: write the mq pin exactly as mq = \"<version>\", not `{line}`"
                    )
                })?;
            return Ok(pin.to_owned());
        }
    }
    Err("Cargo.toml: missing [package.metadata.postgremq] mq = \"<version>\"".into())
}
