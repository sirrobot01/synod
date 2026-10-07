#!/usr/bin/env python3
"""Test the installer with disposable files and a controlled host identity."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

INSTALLER = Path(__file__).resolve().parents[1] / "install.sh"


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="synod-install-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.release = self.root / "release"
        self.release.mkdir()
        self.target = self.root / "bin"
        self.commands = self.root / "commands"
        self.commands.mkdir()
        uname = self.commands / "uname"
        uname.write_text('#!/bin/sh\ncase "$1" in -s) echo "${TEST_OS:-Linux}" ;; -m) echo "${TEST_ARCH:-x86_64}" ;; esac\n')
        uname.chmod(0o755)
        self.env = {**os.environ, "PATH": str(self.commands) + os.pathsep + os.environ["PATH"]}
        (self.release / "release.json").write_text(json.dumps({"version": "v0.1.0-test", "revision": "test"}, indent=2) + "\n")
        (self.release / "synod-linux-amd64").write_bytes(b"amd64-test-binary")
        (self.release / "synod-linux-arm64").write_bytes(b"arm64-test-binary")
        self.checksums()

    def checksums(self):
        (self.release / "SHA256SUMS").write_text("".join(
            hashlib.sha256((self.release / name).read_bytes()).hexdigest() + "  " + name + "\n"
            for name in ("release.json", "synod-linux-amd64", "synod-linux-arm64")
        ))

    def install(self, *extra, network=False):
        command = ["sh", str(INSTALLER), "--version", "v0.1.0-test", "--install-dir", str(self.target)]
        if not network:
            command += ["--from-dir", str(self.release)]
        return subprocess.run(command + list(extra), env=self.env, capture_output=True, text=True, timeout=10)

    def test_both_architectures_and_idempotence(self):
        for machine, arch in (("x86_64", "amd64"), ("aarch64", "arm64")):
            with self.subTest(arch=arch):
                self.env["TEST_ARCH"] = machine
                self.target = self.root / arch
                self.assertEqual(self.install().returncode, 0)
                binary = self.target / "synod"
                self.assertEqual(binary.read_bytes(), (self.release / ("synod-linux-" + arch)).read_bytes())
                self.assertEqual(binary.stat().st_mode & 0o777, 0o755)
                inode = binary.stat().st_ino
                self.assertEqual(self.install().returncode, 0)
                self.assertEqual(binary.stat().st_ino, inode)
                self.assertEqual(list(self.target.glob(".synod-install.*")), [])

    def test_checksum_failure_keeps_existing_binary(self):
        self.assertEqual(self.install().returncode, 0)
        binary = self.target / "synod"
        original = binary.read_bytes()
        (self.release / "synod-linux-amd64").write_bytes(b"corrupt")
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checksum differs", result.stderr)
        self.assertEqual(binary.read_bytes(), original)
        self.assertEqual(list(self.target.glob(".synod-install.*")), [])

    def test_upgrade_keeps_previous_binary_and_open_inode(self):
        self.assertEqual(self.install().returncode, 0)
        binary = self.target / "synod"
        previous = self.target / "synod.previous"
        original = binary.read_bytes()
        (self.release / "synod-linux-amd64").write_bytes(b"upgraded-binary")
        self.checksums()
        with binary.open("rb") as running:
            result = self.install("--upgrade")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(binary.read_bytes(), b"upgraded-binary")
            self.assertEqual(previous.read_bytes(), original)
            self.assertEqual(running.read(), original)
        inode = binary.stat().st_ino
        self.assertEqual(self.install("--upgrade").returncode, 0)
        self.assertEqual(binary.stat().st_ino, inode)
        self.assertEqual(previous.read_bytes(), original)
        (self.release / "synod-linux-amd64").write_bytes(b"corrupt")
        self.assertNotEqual(self.install("--upgrade").returncode, 0)
        self.assertEqual(binary.read_bytes(), b"upgraded-binary")
        self.assertEqual(previous.read_bytes(), original)
        self.assertEqual(list(self.target.glob(".synod-install.*")), [])

    def test_upgrade_failure_before_replace_keeps_old_binary(self):
        self.assertEqual(self.install().returncode, 0)
        binary = self.target / "synod"
        original = binary.read_bytes()
        (self.release / "synod-linux-amd64").write_bytes(b"upgraded-binary")
        self.checksums()
        command = self.commands / "mv"
        command.write_text('#!/bin/sh\ncase "$2" in */synod-linux-*) exit 1 ;; esac\nexec /bin/mv "$@"\n')
        command.chmod(0o755)
        self.assertNotEqual(self.install("--upgrade").returncode, 0)
        self.assertEqual(binary.read_bytes(), original)
        self.assertEqual((self.target / "synod.previous").read_bytes(), original)
        self.assertEqual(list(self.target.glob(".synod-install.*")), [])
        command.unlink()
        result = self.install("--upgrade")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(binary.read_bytes(), b"upgraded-binary")
        self.assertEqual((self.target / "synod.previous").read_bytes(), original)

    def test_upgrade_refuses_missing_binary_unsafe_backup_and_held_lock(self):
        self.assertNotEqual(self.install("--upgrade").returncode, 0)
        self.assertEqual(self.install().returncode, 0)
        binary = self.target / "synod"
        original = binary.read_bytes()
        previous = self.target / "synod.previous"
        outside = self.root / "outside"
        outside.write_bytes(b"keep")
        previous.symlink_to(outside)
        self.assertNotEqual(self.install("--upgrade").returncode, 0)
        self.assertEqual(outside.read_bytes(), b"keep")
        previous.unlink()
        previous.mkdir()
        self.assertNotEqual(self.install("--upgrade").returncode, 0)
        previous.rmdir()
        lock = self.target / ".synod-install.lock"
        lock.mkdir()
        self.assertNotEqual(self.install("--upgrade").returncode, 0)
        self.assertTrue(lock.is_dir())
        self.assertEqual(binary.read_bytes(), original)
        lock.rmdir()

    def test_rejects_different_binary_and_symlink(self):
        self.target.mkdir()
        binary = self.target / "synod"
        binary.write_bytes(b"existing")
        self.assertNotEqual(self.install().returncode, 0)
        self.assertEqual(binary.read_bytes(), b"existing")
        binary.unlink()
        outside = self.root / "outside"
        outside.write_bytes(b"keep")
        binary.symlink_to(outside)
        self.assertNotEqual(self.install().returncode, 0)
        self.assertEqual(outside.read_bytes(), b"keep")
        self.assertTrue(binary.is_symlink())

    def test_rejects_wrong_release_and_ambiguous_checksums(self):
        metadata = self.release / "release.json"
        metadata.write_text(metadata.read_text().replace("v0.1.0-test", "v0.2.0"))
        self.checksums()
        self.assertNotEqual(self.install().returncode, 0)
        self.assertFalse((self.target / "synod").exists())
        metadata.write_text(metadata.read_text().replace("v0.2.0", "v0.1.0-test"))
        self.checksums()
        manifest = self.release / "SHA256SUMS"
        manifest.write_text(manifest.read_text() * 2)
        self.assertNotEqual(self.install().returncode, 0)
        self.assertFalse((self.target / "synod").exists())

    def test_refuses_unsupported_platform_and_invalid_options(self):
        for setting, value in (("TEST_OS", "Darwin"), ("TEST_ARCH", "armv7l")):
            self.env[setting] = value
            self.assertNotEqual(self.install().returncode, 0)
            self.env.pop(setting)
        for args in (("--version", "../../unsafe"), ("--version", "v0.1.0-test\nunsafe"), ("--install-dir", "relative"), ("--from-dir", "relative"), ("--version",)):
            self.assertNotEqual(self.install(*args).returncode, 0)
        self.assertFalse(self.target.exists())

    def test_https_download_and_failed_download(self):
        curl = self.commands / "curl"
        curl.write_text('''#!/bin/sh
set -eu
printf '%s\\n' "$@" >> "$TEST_CURL_ARGS"
while [ "$#" -gt 0 ]; do
    case "$1" in
        https://*) file=${1##*/} ;;
        --output) shift; output=$1 ;;
    esac
    shift
done
[ "${TEST_DOWNLOAD_FAIL:-0}" = 0 ] || exit 22
cp "$TEST_RELEASE/$file" "$output"
''')
        curl.chmod(0o755)
        self.env.update(TEST_RELEASE=str(self.release), TEST_CURL_ARGS=str(self.root / "curl-args"))
        self.assertEqual(self.install(network=True).returncode, 0)
        arguments = (self.root / "curl-args").read_text()
        self.assertIn("--proto-redir\n=https", arguments)
        self.assertIn("/v0.1.0-test/synod-linux-amd64", arguments)
        self.assertNotEqual(self.install("--base-url", "http://insecure", network=True).returncode, 0)
        self.target = self.root / "failed-download"
        self.env["TEST_DOWNLOAD_FAIL"] = "1"
        self.assertNotEqual(self.install(network=True).returncode, 0)
        self.assertFalse((self.target / "synod").exists())
        self.assertEqual(list(self.target.glob(".synod-install.*")), [])


if __name__ == "__main__":
    unittest.main()
