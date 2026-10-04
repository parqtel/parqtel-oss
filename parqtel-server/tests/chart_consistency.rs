//! A consistency test may panic; the workspace ban on `panic` is about
//! production code paths, not test assertions.
#![allow(clippy::panic, clippy::expect_used, clippy::unwrap_used)]

//! Guards against the chart's default image drifting from what the release
//! workflow actually publishes.
//!
//! The published image is `ghcr.io/${{ github.repository }}` — the repository
//! *slug*. The chart's `image.repository` default is a hand-maintained string.
//! When the two disagree, `helm install` succeeds and the pod then sits in
//! ImagePullBackOff pulling an image nobody published, which is exactly the
//! failure this asserts against.

use std::path::PathBuf;

/// Fully-qualified image reference the release workflow pushes.
fn published_image_from_workflow(workflow: &str) -> String {
    let line = workflow
        .lines()
        .find(|l| l.trim_start().starts_with("IMAGE_NAME:"))
        .unwrap_or_else(|| panic!("release.yml has no IMAGE_NAME"));
    // IMAGE_NAME: ${{ github.repository }}
    let expr = line
        .split_once(':')
        .map(|(_, v)| v.trim())
        .unwrap_or_default();
    assert_eq!(
        expr, "${{ github.repository }}",
        "release.yml now derives IMAGE_NAME some other way; update this test to match"
    );
    format!("ghcr.io/{}", repo_slug())
}

/// The `<owner>/<repo>` slug the workflow's `${{ github.repository }}` resolves
/// to. Hard-coded alongside the workflow assertion above so a rename fails the
/// test loudly rather than silently comparing the wrong pair.
fn repo_slug() -> &'static str {
    "parqtel/parqtel-oss"
}

/// `image.repository` as written in the chart's values.yaml.
fn chart_default_image(values: &str) -> String {
    let mut in_image = false;
    for line in values.lines() {
        let t = line.trim();
        if t.starts_with("image:") && line.starts_with("image:") {
            in_image = true;
            continue;
        }
        if in_image {
            if let Some(v) = t.strip_prefix("repository:") {
                return v.trim().trim_matches('"').to_string();
            }
            // Left the image block.
            if !line.starts_with(' ') && !t.is_empty() {
                break;
            }
        }
    }
    panic!("values.yaml has no image.repository");
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate has a parent")
        .to_path_buf()
}

#[test]
fn chart_default_image_matches_release_workflow() {
    let root = repo_root();
    let workflow = std::fs::read_to_string(root.join(".github/workflows/release.yml"))
        .expect("read release.yml");
    let values =
        std::fs::read_to_string(root.join("charts/parqtel/values.yaml")).expect("read values.yaml");

    let published = published_image_from_workflow(&workflow);
    let chart = chart_default_image(&values);

    assert_eq!(
        chart, published,
        "chart image.repository ({chart}) does not match the published image \
         ({published}); an install would ImagePullBackOff"
    );
}

#[test]
fn chart_app_version_is_a_semver_tag() {
    // `image.tag` defaults to "", which resolves to Chart.appVersion. The
    // release workflow tags images with the workspace version, so the two must
    // agree for a bare `helm install --version X` to work.
    let chart =
        std::fs::read_to_string(repo_root().join("charts/parqtel/Chart.yaml")).expect("read Chart");
    let app_version = chart
        .lines()
        .find(|l| l.trim_start().starts_with("appVersion:"))
        .and_then(|l| {
            l.split_once(':')
                .map(|(_, v)| v.trim().trim_matches('"').to_string())
        })
        .expect("Chart.yaml has appVersion");
    let workspace =
        std::fs::read_to_string(repo_root().join("Cargo.toml")).expect("read Cargo.toml");
    let version = workspace
        .lines()
        .find(|l| l.starts_with("version = "))
        .and_then(|l| l.split('"').nth(1))
        .expect("workspace version in Cargo.toml")
        .to_string();
    assert_eq!(
        app_version, version,
        "Chart.appVersion must equal the workspace version, since image.tag \
         defaults to it"
    );
}
