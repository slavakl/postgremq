#!/usr/bin/env python3
"""Check that the checked-out source is a consistent release of one component.

    scripts/release/verify_release.py rust/v0.2.0
    scripts/release/verify_release.py --component rust     # the manifest's version

Used on release PRs, by the release guard before tags are created, and by the
publish workflow on the release tag. Checks, for the component and version:

- the Release Please manifest and the component's own version source agree
  (mq/VERSION, Cargo.toml/Cargo.lock, package.json/package-lock.json; Go
  modules are versioned by their tag only);
- the component's CHANGELOG.md has an entry for the version;
- mq: sql/latest.sql records the latest migration (the schema version
  postgremq.info() reports for a fresh install);
- dependencies are released, explicit versions: the Go client's
  postgremq.dev/mq, the CLI's postgremq.dev/postgremq-go (tags in this
  branch's history, with go.sum hashes), and the mq schema version N the Rust
  and TypeScript clients pin: migrations 1..N must be part of a released mq
  version and byte-identical to the ones they embed;
- with --tag-exists, the tag exists and points at HEAD.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# component (tag prefix) -> package path in release-please-config.json
COMPONENTS = {
    'mq': 'mq',
    'postgremq-go': 'postgremq-go',
    'cmd/postgremq': 'cmd/postgremq',
    'rust': 'postgremq-rs',
    'npm': 'postgremq-ts',
}

UP = re.compile(r'^(\d+)_(.+)\.up\.sql$')


def git(*args: str, check: bool = True) -> str:
    result = subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f'git {" ".join(args)}: {result.stderr.strip()}')
    return result.stdout if result.returncode == 0 else ''


def released(tag: str) -> bool:
    """Whether `tag` exists and is in this branch's history."""
    if not git('rev-parse', '-q', '--verify', f'refs/tags/{tag}', check=False):
        return False
    return subprocess.run(['git', '-C', str(ROOT), 'merge-base', '--is-ancestor', tag, 'HEAD'],
                          capture_output=True).returncode == 0


def migrations(directory: Path) -> list[tuple[int, str, str]]:
    found = []
    for path in directory.iterdir():
        match = UP.match(path.name)
        if match:
            found.append((int(match.group(1)), match.group(2), path.name))
    return sorted(found)


def go_requirement(go_mod: Path, module: str) -> str | None:
    match = re.search(rf'^\s*(?:require\s+)?{re.escape(module)} (v\S+)', go_mod.read_text(), re.M)
    return match.group(1) if match else None


class Checker:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def expect(self, ok: bool, message: str) -> None:
        if not ok:
            self.errors.append(message)

    def common(self, component: str, version: str, tag_exists: bool) -> None:
        path = COMPONENTS[component]
        manifest = json.loads((ROOT / '.release-please-manifest.json').read_text())
        self.expect(manifest.get(path) == version,
                    f'.release-please-manifest.json has {path} = {manifest.get(path)!r}, not {version!r}')
        changelog = (ROOT / path / 'CHANGELOG.md').read_text()
        self.expect(re.search(rf'^##\s+\[?{re.escape(version)}\]?[\s(]', changelog, re.M) is not None,
                    f'{path}/CHANGELOG.md has no entry for {version}')
        if tag_exists:
            tag = f'{component}/v{version}'
            head = git('rev-parse', 'HEAD').strip()
            target = git('rev-parse', f'{tag}^{{commit}}', check=False).strip()
            self.expect(target == head, f'tag {tag} does not point at HEAD ({target or "missing"} != {head})')

    def unique_migration_numbers(self) -> None:
        numbers = [m[0] for m in migrations(ROOT / 'mq' / 'migrations')]
        duplicates = sorted({n for n in numbers if numbers.count(n) > 1})
        self.expect(not duplicates, f'mq/migrations has more than one migration numbered {duplicates}')

    def go_license(self, module_dir: str) -> None:
        # The Go module proxy adds the repository's LICENSE to a nested module
        # without one, so a local zip (and its go.sum hash) would differ.
        self.expect((ROOT / module_dir / 'LICENSE').exists(), f'{module_dir}/LICENSE is missing')

    def mq(self, version: str) -> None:
        mq = ROOT / 'mq'
        self.go_license('mq')
        recorded = (mq / 'VERSION').read_text().strip()
        self.expect(recorded == version, f'mq/VERSION is {recorded!r}, not {version!r}')
        found = migrations(mq / 'migrations')
        latest_sql = (mq / 'sql' / 'latest.sql').read_text()
        number = re.search(r'INSERT INTO postgremq\.postgremq_migrations \(version, dirty\) VALUES \((\d+), false\);',
                           latest_sql)
        self.expect(found and number is not None and int(number.group(1)) == found[-1][0],
                    f'mq/sql/latest.sql records migration {number and number.group(1)}, '
                    f'not the latest ({found and found[-1][0]})')

    def go_dependency(self, module_dir: str, dependency: str, dependency_dir: str) -> None:
        self.go_license(module_dir)
        go_mod = ROOT / module_dir / 'go.mod'
        required = go_requirement(go_mod, dependency)
        if required is None:
            self.errors.append(f'{module_dir}/go.mod does not require {dependency}')
            return
        tag = f'{dependency_dir}/{required}'
        self.expect(released(tag), f'{module_dir} requires {dependency} {required}, which is not released '
                                   f'(no tag {tag} in this history); release it first')
        sums = (ROOT / module_dir / 'go.sum').read_text()
        for entry in (f'{dependency} {required} h1:', f'{dependency} {required}/go.mod h1:'):
            self.expect(entry in sums, f'{module_dir}/go.sum lacks "{entry}..."; '
                                       f'run scripts/release/go-standalone.sh --write {module_dir} go mod tidy after the release')

    def pinned_schema(self, package: str, pin: int | None) -> None:
        if not pin:
            self.errors.append(f'{package}: no mq schema pin')
            return
        found = migrations(ROOT / 'mq' / 'migrations')
        bundled = [m for m in found if m[0] <= pin]
        if not any(m[0] == pin for m in found):
            self.errors.append(f'{package} pins mq schema {pin}, but mq/migrations has no migration {pin}')
            return
        released_in = None
        for tag in git('tag', '--merged', 'HEAD', '--list', 'mq/v*').split():
            at_tag = {name for name in git('ls-tree', '--name-only', f'{tag}:mq/migrations').split() if UP.match(name)}
            if any(UP.match(name) and int(UP.match(name).group(1)) == pin for name in at_tag):
                released_in = (tag, at_tag)
                break
        if released_in is None:
            self.errors.append(f'{package} pins mq schema {pin}, which no mq release in this history contains; '
                               'release mq first, then pin a released schema version')
            return
        tag, at_tag = released_in
        for _, _, name in bundled:
            self.expect(name in at_tag, f'{package} would embed {name}, which {tag} does not contain')
            if name in at_tag:
                self.expect((ROOT / 'mq' / 'migrations' / name).read_text() == git('show', f'{tag}:mq/migrations/{name}'),
                            f'mq/migrations/{name} differs from the released {tag}')

    def rust(self, version: str) -> None:
        cargo = (ROOT / 'postgremq-rs' / 'Cargo.toml').read_text()
        declared = re.search(r'^version = "([^"]+)"', cargo, re.M)
        self.expect(declared is not None and declared.group(1) == version,
                    f'postgremq-rs/Cargo.toml version is {declared and declared.group(1)!r}, not {version!r}')
        lock = (ROOT / 'postgremq-rs' / 'Cargo.lock').read_text()
        self.expect(f'name = "postgremq"\nversion = "{version}"' in lock,
                    f'postgremq-rs/Cargo.lock does not record postgremq {version}')
        pin = re.search(r'^\[package\.metadata\.postgremq\]\s*\nmq-schema = (\d+)$', cargo, re.M)
        self.pinned_schema('postgremq-rs', pin and int(pin.group(1)))

    def npm(self, version: str) -> None:
        package = json.loads((ROOT / 'postgremq-ts' / 'package.json').read_text())
        self.expect(package.get('version') == version,
                    f'postgremq-ts/package.json version is {package.get("version")!r}, not {version!r}')
        lock = json.loads((ROOT / 'postgremq-ts' / 'package-lock.json').read_text())
        self.expect(lock.get('version') == version and lock.get('packages', {}).get('', {}).get('version') == version,
                    f'postgremq-ts/package-lock.json does not record {version}')
        pin = package.get('postgremq', {}).get('mq-schema')
        self.pinned_schema('postgremq-ts', pin if isinstance(pin, int) else None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('tag', nargs='?', help='a release tag, e.g. rust/v0.2.0')
    target.add_argument('--component', choices=sorted(COMPONENTS), help="check the manifest's version")
    parser.add_argument('--tag-exists', action='store_true', help='also require the tag to point at HEAD')
    args = parser.parse_args()

    if args.tag:
        component, sep, version = args.tag.rpartition('/v')
        if not sep or component not in COMPONENTS:
            parser.error(f'not a release tag: {args.tag} (want one of {", ".join(f"{c}/vX.Y.Z" for c in COMPONENTS)})')
    else:
        component = args.component
        version = json.loads((ROOT / '.release-please-manifest.json').read_text())[COMPONENTS[component]]

    checker = Checker()
    checker.common(component, version, args.tag_exists)
    checker.unique_migration_numbers()
    if component == 'mq':
        checker.mq(version)
    elif component == 'postgremq-go':
        checker.go_dependency('postgremq-go', 'postgremq.dev/mq', 'mq')
    elif component == 'cmd/postgremq':
        checker.go_dependency('cmd/postgremq', 'postgremq.dev/postgremq-go', 'postgremq-go')
    elif component == 'rust':
        checker.rust(version)
    elif component == 'npm':
        checker.npm(version)

    if checker.errors:
        print(f'{component} {version} is not a consistent release:', file=sys.stderr)
        for error in checker.errors:
            print(f'  - {error}', file=sys.stderr)
        return 1
    print(f'{component} {version}: consistent release')
    return 0


if __name__ == '__main__':
    sys.exit(main())
