-- mq 0.2.0-e2e.1 release stamp, generated in the release PR by
-- mq/scripts/stamp_release.py. Do not edit: released migrations are immutable.
-- Records the implementation version that postgremq.info() reports.
CREATE OR REPLACE FUNCTION postgremq.info() RETURNS jsonb
LANGUAGE sql STABLE
AS $$
    SELECT jsonb_build_object('db_version', '0.2.0-e2e.1', 'protocol_major', 1)
$$;
