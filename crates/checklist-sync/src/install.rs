//! Fetching the `cloudsync` extension.
//!
//! The binary is **not** vendored into this repository. checklist-tui is MIT;
//! the extension is Elastic License 2.0 (with an open-source grant that covers
//! this use). Keeping the two apart means anyone who installs the crate gets
//! only MIT bytes, and the licence question never arises for them.
//!
//! Bumping [`CLOUDSYNC_VERSION`] requires bumping [`CLOUDSYNC_SHA256`] with it,
//! and the same pair appears in `crates/checklist-relay/Containerfile`.

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

pub const CLOUDSYNC_VERSION: &str = "1.1.2";
pub const CLOUDSYNC_SHA256: &str =
    "1aaad0a0a891a5fdae2bb95ca042a535165fd44aca2bf67cc2827285e5ed3c99";

/// Filename the extension is installed under, inside the config directory.
pub const EXTENSION_FILENAME: &str = "cloudsync.so";

fn release_url() -> String {
    format!(
        "https://github.com/sqliteai/sqlite-sync/releases/download/{v}/cloudsync-linux-x86_64-{v}.tar.gz",
        v = CLOUDSYNC_VERSION
    )
}

/// Download, verify and unpack the extension into `dir`.
///
/// Verification is against the pinned checksum and happens **before** anything
/// is written to the destination: this file gets loaded as native code into the
/// process, so an unverified download must never reach disk where a later run
/// might pick it up.
pub fn install_extension(dir: &Path) -> Result<PathBuf> {
    let url = release_url();
    println!("Downloading cloudsync {CLOUDSYNC_VERSION}...");

    let mut response = ureq::get(&url)
        .call()
        .with_context(|| format!("failed to download {url}"))?;

    let mut archive = Vec::new();
    response
        .body_mut()
        .as_reader()
        .read_to_end(&mut archive)
        .context("failed to read the downloaded archive")?;

    let digest: String = Sha256::digest(&archive)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if digest != CLOUDSYNC_SHA256 {
        bail!(
            "checksum mismatch for {url}\n  expected {CLOUDSYNC_SHA256}\n  got      {digest}\n\
             refusing to install"
        );
    }
    println!("Checksum verified ({} bytes).", archive.len());

    let library = extract_library(&archive)?;

    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create {}", dir.display()))?;
    let dest = dir.join(EXTENSION_FILENAME);

    // Write beside the target and rename, so a failure here cannot leave a
    // half-written shared library in place.
    let tmp = dir.join(format!("{EXTENSION_FILENAME}.tmp"));
    std::fs::write(&tmp, &library)
        .with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, &dest)
        .with_context(|| format!("failed to install {}", dest.display()))?;

    println!("Installed {}", dest.display());
    Ok(dest)
}

/// Pull `cloudsync.so` out of the release tarball.
///
/// Hand-rolled rather than pulling in tar+flate2: the archive has a known,
/// single-member shape, and gzip-then-tar is little more than a header walk.
fn extract_library(archive: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = flate2::read::GzDecoder::new(archive);
    let mut tar = Vec::new();
    decoder
        .read_to_end(&mut tar)
        .context("failed to decompress the release archive")?;

    let mut offset = 0usize;
    while offset + 512 <= tar.len() {
        let header = &tar[offset..offset + 512];
        // Two consecutive zero blocks terminate a tar archive.
        if header.iter().all(|&b| b == 0) {
            break;
        }

        let name = std::str::from_utf8(&header[0..100])
            .unwrap_or("")
            .trim_end_matches('\0')
            .to_string();
        let size_field = std::str::from_utf8(&header[124..136])
            .unwrap_or("")
            .trim_end_matches(['\0', ' ']);
        let size = usize::from_str_radix(size_field.trim(), 8)
            .with_context(|| format!("bad size field for tar member {name:?}"))?;

        let start = offset + 512;
        let end = start + size;
        if end > tar.len() {
            bail!("truncated tar member {name:?}");
        }

        if Path::new(&name).file_name().and_then(|n| n.to_str()) == Some(EXTENSION_FILENAME) {
            return Ok(tar[start..end].to_vec());
        }

        // Members are padded to a 512-byte boundary.
        offset = start + size.div_ceil(512) * 512;
    }

    bail!("{EXTENSION_FILENAME} not found in the release archive")
}
