# Changelog

## v1.2.1 (2025-02-12)

* Run `setup` callback on Broadway startup

## v1.2.0 (2025-02-10)

* Do not clean up persistent terms on shutdown
* Add format_discarded callback
* Allow different config storages

## v1.1.0 (2024-06-21)

### Bug fix

  * No longer set demand to `:accumulate` when draining, for compatibility with GenStage v1.2+. This means that any polling implementation must implement the `prepare_for_draining` callback and stop polling messages. You can check how [BroadwaySQS](https://github.com/dashbitco/broadway_sqs/commit/5b8f18a78e4760b5fcc839ad576be8c63345add0) tackles this problem as an example

### Enhancements

  * Log leaked trapped exits

## v1.0.7 (2023-04-22)

### Enhancements

  * Relax `nimble_options` dependency
  * Improve documentation and error messages

## v1.0.6 (2023-01-19)

### Bug fixes

  * Remove the assumption a cancelled timer has been delivered

## v1.0.5 (2022-11-06)

### Bug fixes

  * Fix NoopAcknowledger metadata name

## v1.0.4 (2022-11-05)

### Enhancements

  * Add `init` convenience function to acknowledgers
  * Allow `:reset` option when calling `update_rate_limiting`. When set to true, the rate limit counter and interval is immediately reset
  * Add the producer to the telemetry metadata
  * Support custom function in `:batch_size` for customized batch splitting logic

## v1.0.3 (2022-03-18)

### Bug fixes

  * Move the `process_name/2` callback to the `Broadway` behaviour where it belongs

## v1.0.2 (2022-01-12)

### Enhancements

  * Also allow `nimble_options ~> 0.4.0`

## v1.0.1 (2021-10-12)

### Bug fixes

  * Make sure `processors` and `batch_processors` demands are properly shuffled
  * Ensure proper messages metadata for telemetry events on `Broadway.Message.failed/2`

## v1.0.0 (2021-08-30)

Broadway v1.0 requires Erlang/OTP 21.3+.

### Backwards incompatible changes

  * Remove `Broadway.TermStorage` now that we have Broadway topology information on the producer init callback
  * Rename `:events` to `:messages` in batcher telemetry event
  * Remove `:time` from "stop" telemetry event measurements
  * Rename `:time` to `:system_time` in telemetry event measurements
  * Rename `[:broadway, :consumer, *]` to `[:broadway, :batch_processor, *]` in telemetry event

### Enhancements

  * Add `Broadway.Message.put_data/2`
  * Add `Broadway.stop/1`
  * Add `Broadway.all_running/0`
  * Add `Broadway.topology/1`
  * Add ack configuration to `Broadway.test_message/3` and `Broadway.test_batch/3`
  * Allow Broadway :via tuples as broadway names
  * Enrich telemetry events with metadata

## v0.6.2 (2020-08-17)

  * Make `Broadway.Producer` public
  * Add optional `prepare_messages` callback

## v0.6.1 (2020-06-02)

  * Rename `:failure` Telemetry event to `:exception` so it conforms to the telemetry specification
  * Deprecate `Broadway.test_messages/3` in favor of `Broadway.test_message/3` and `Broadway.test_batch/3`

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
