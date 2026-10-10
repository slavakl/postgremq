#!/usr/bin/env python3
"""Stamp an mq release: make upgrades and fresh installs report its version.

Run in the mq release PR, after Release Please has written the new version to
mq/VERSION:

    python3 mq/scripts/stamp_release.py 0.3.0

It adds the migration `NNNNNN_release_v0_3_0.up.sql`, which redefines
postgremq.info() with the new db_version, and updates sql/latest.sql to match:
the db_version in its info() and the migration number its version table
records. Running it again for the same version changes nothing.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SEMVER = re.compile(r'^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$')
MIGRATION = re.compile(r'^(\d+)_(.+)\.up\.sql$')
INFO = re.compile(r"jsonb_build_object\('db_version', '([^']*)', 'protocol_major', (\d+)\)")
RECORDED = re.compile(
    r'(INSERT INTO postgremq\.postgremq_migrations \(version, dirty\) VALUES \()(\d+)(, false\);)')

STAMP = """\
-- mq {version} release stamp, generated in the release PR by
-- mq/scripts/stamp_release.py. Do not edit: released migrations are immutable.
-- Records the implementation version that postgremq.info() reports.
CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
LANGUAGE sql STABLE
AS $$
    SELECT jsonb_build_object('db_version', '{version}', 'protocol_major', {major})
$$;
"""

DOWN = """\
-- Down migrations are not supported for PostgreMQ.
-- To remove an installation and all queue data, explicitly run:
-- DROP SCHEMA postgremq CASCADE;
"""


def stamp_name(version: str) -> str:
    return 'release_v' + re.sub(r'[^0-9A-Za-z]', '_', version)


def stamp(mq_dir: Path, version: str) -> list[str]:
    """Stamps `version`; returns the files it changed."""
    if not SEMVER.match(version):
        raise SystemExit(f'not a semantic version: {version!r}')
    recorded_version = (mq_dir / 'VERSION').read_text().strip()
    if recorded_version != version:
        raise SystemExit(f'mq/VERSION is {recorded_version!r}, not {version!r}')

    migrations_dir = mq_dir / 'migrations'
    migrations = {}
    numbers: dict[int, str] = {}
    for path in migrations_dir.iterdir():
        match = MIGRATION.match(path.name)
        if match:
            number = int(match.group(1))
            if number in numbers:
                raise SystemExit(f'two migrations numbered {number}: {numbers[number]} and {path.name}')
            numbers[number] = path.name
            migrations[match.group(2)] = number
    if not migrations:
        raise SystemExit(f'no migrations in {migrations_dir}')

    latest_path = mq_dir / 'sql' / 'latest.sql'
    latest = latest_path.read_text()
    info = INFO.findall(latest)
    if len(info) != 1:
        raise SystemExit(f'expected one info() definition in {latest_path}, found {len(info)}')
    major = info[0][1]
    if len(RECORDED.findall(latest)) != 1:
        raise SystemExit(f'expected one recorded migration version in {latest_path}')

    changed = []
    name = stamp_name(version)
    number = migrations.get(name)
    if number is None:
        number = max(migrations.values()) + 1
        up = migrations_dir / f'{number:06d}_{name}.up.sql'
        up.write_text(STAMP.format(version=version, major=major))
        (migrations_dir / f'{number:06d}_{name}.down.sql').write_text(DOWN)
        changed.append(str(up))
    elif number != max(migrations.values()):
        raise SystemExit(f'the stamp for {version} is not the latest migration')

    updated = INFO.sub(f"jsonb_build_object('db_version', '{version}', 'protocol_major', {major})", latest)
    updated = RECORDED.sub(lambda m: f'{m.group(1)}{number}{m.group(3)}', updated)
    if updated != latest:
        latest_path.write_text(updated)
        changed.append(str(latest_path))
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('version', help='the mq version being released (as in mq/VERSION)')
    parser.add_argument('--mq-dir', type=Path, default=Path(__file__).resolve().parent.parent,
                        help='the mq directory (default: this script\'s)')
    args = parser.parse_args()
    changed = stamp(args.mq_dir, args.version)
    for path in changed:
        print(f'updated {path}')
    if not changed:
        print(f'mq {args.version} is already stamped')


if __name__ == '__main__':
    sys.exit(main())
