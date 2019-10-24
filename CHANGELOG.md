# Changelog

## v0.5.0-dev

  * Deprecate `:producers` in favor of a single `:producer` key
  * Add `Broadway.Message.configure_ack/3`
  * Add `Broadway.Message.ack_immediately/1`
  * Add the `c:Broadway.handle_failed/2` callback

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
