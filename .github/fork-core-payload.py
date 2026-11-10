#!/usr/bin/env python3
"""Builds the core payloads Clash Verge's update component publishes, one per Clash Verge
platform: a zip of the core and a manifest, signed with the fork's updater key.

usage: fork-core-payload.py <release dir> <version> <commit>

Signing runs `tauri signer sign`, which reads TAURI_SIGNING_PRIVATE_KEY and
TAURI_SIGNING_PRIVATE_KEY_PASSWORD. The `.sig` beside each payload holds one signature per line.
"""

import datetime
import gzip
import hashlib
import json
import os
import subprocess
import sys
import zipfile

TAURI_CLI = "@tauri-apps/cli@2.12.3"

# mihomo asset -> Clash Verge platform. darwin-amd64-v2 has no Clash Verge build.
PLATFORMS = {
    "mihomo-windows-amd64-v2": "windows-x86_64",
    "mihomo-linux-amd64-v2": "linux-x86_64",
    "mihomo-linux-arm64": "linux-aarch64",
    "mihomo-darwin-arm64": "darwin-aarch64",
}


def read_core(directory, asset, version, windows):
    if windows:
        with zipfile.ZipFile(os.path.join(directory, f"{asset}-{version}.zip")) as archive:
            return archive.read(f"{asset}.exe")
    with gzip.open(os.path.join(directory, f"{asset}-{version}.gz")) as file:
        return file.read()


def main(directory, version, commit):
    built_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for asset, platform in PLATFORMS.items():
        windows = platform.startswith("windows")
        core = read_core(directory, asset, version, windows)
        name = "verge-mihomo.exe" if windows else "verge-mihomo"
        digest = hashlib.sha256(core).hexdigest()
        manifest = {
            "format": 1,
            "version": version,
            "platform": platform,
            "kind": "core",
            "files": [{"path": name, "sha256": digest, "role": "core"}],
            # One program serves both of Clash Verge's core names.
            "cores": [
                {"name": core_name, "version": version, "sha256": digest}
                for core_name in ("verge-mihomo", "verge-mihomo-alpha")
            ],
            "commit": commit,
            "built_at": built_at,
        }
        out = os.path.join(directory, f"{asset}-{version}.core.zip")
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100755 << 16
            archive.writestr(entry, core)
        subprocess.run(["npx", "--yes", TAURI_CLI, "signer", "sign", out], check=True)
        signature = out + ".sig"
        with open(signature, encoding="utf-8") as file:
            value = file.read().strip()
        with open(signature, "w", encoding="utf-8") as file:
            file.write(value + "\n")
        print(f"{out}: {digest}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:])
