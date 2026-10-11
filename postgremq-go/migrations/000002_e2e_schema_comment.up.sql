-- Migration 2: e2e schema comment.
--
-- Make the same change in mq/sql/latest.sql (a fresh install must equal
-- the migrations), then run `make sync`. Once merged to main this file
-- never changes.
COMMENT ON SCHEMA postgremq IS 'PostgreMQ message queue (e2e round 3)';
