# Changelog

## v0.6.0 (2020-02-13)

  * Deprecate `:stages` in favor of `:concurrency` for clarity
  * Do not validate `:batcher` if message failed
  * Add support for rate limiting producers
  * Support returning state in `c:Broadway.Producer.prepare_for_draining/1`
  * Emit telemetry events
  * Add Kafka guide

## v0.5.0 (2019-11-04)

  * Deprecate `:producers` in favor of a single `:producer` key
  * Add `Broadway.Message.configure_ack/3`
  * Add `Broadway.Message.ack_immediately/1`
  * Add `Broadway.producer_names/1`
  * Add the `c:Broadway.handle_failed/2` optional callback which is invoked with failed messages
  * Add `:crash_reason` to Logger reports metadata
  * Add `c:Broadway.Producer.prepare_for_start/2` optional callback which allows producers to customize the topology
  * Support `partition_by` in processors and batchers
  * Log if `handle_batch` returns less messages than expected

## v0.4.0 (2019-08-05)

  * Add `:batch_mode` to `Broadway.test_messages/3` to control how test messages are flushed
  * Add `Broadway.DummyProducer` for testing
  * Append .Broadway to module prefixes to avoid potential naming conflicts

## v0.3.0 (2019-04-26)

  * Add `metadata` field to the `Message` struct so clients can append extra information

## v0.2.0 (2019-04-04)

  * `Broadway.Message.put_partition/2` has been renamed to `Broadway.Message.put_batch_key/2`
  * Allow `Broadway.Producer` to `prepare_for_draining/1`
  * Allow pipelines without batchers

## v0.1.0 (2019-02-19)

  * Initial release
