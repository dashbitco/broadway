# Introduction

`Broadway` is a library for building concurrent and multi-stage data ingestion and data processing pipelines with Elixir. Broadway pipelines are concurrent and robust, thanks to the Erlang VM and its actors. It features:

  * Back-pressure
  * Automatic acknowledgements at the end of the pipeline
  * Batching
  * Fault tolerance
  * Graceful shutdown
  * Built-in testing
  * Custom failure handling
  * Ordering and partitioning
  * Rate-limiting
  * Metrics

## Official Producers

Currently we officially support four Broadway producers:

  * Amazon SQS: [Source](https://github.com/dashbitco/broadway_sqs) - [Guide](amazon-sqs.md)
  * Apache Kafka: [Source](https://github.com/dashbitco/broadway_kafka) - [Guide](apache-kafka.md)
  * Google Cloud Pub/Sub: [Source](https://github.com/dashbitco/broadway_cloud_pub_sub) - [Guide](google-cloud-pubsub.md)
  * RabbitMQ: [Source](https://github.com/dashbitco/broadway_rabbitmq) - [Guide](rabbitmq.md)

The guides links above will help you get started with your adapter of choice. For API reference, you can check out the `Broadway` module.

## Non-official (Off-Broadway) Producers

For those interested in rolling their own Broadway Producers (which we actively encourage!), we recommend using the `OffBroadway` namespace, mirroring the [Off-Broadway theaters](https://en.wikipedia.org/wiki/Off-Broadway). For example, if you want to publish your own integration with Amazon SQS, you can package it as `off_broadway_sqs`, which uses the `OffBroadway.SQS` namespace.

The following Off-Broadway libraries are available (feel free to send a PR adding your own in alphabetical order):

  * [off_broadway_amqp10](https://github.com/highmobility/off_broadway_amqp10): [Guide](https://hexdocs.pm/off_broadway_amqp10/)
  * [off_broadway_elasticsearch](https://github.com/jonlunsford/off_broadway_elasticsearch): [Guide](https://hexdocs.pm/off_broadway_elasticsearch/)
  * [off_broadway_emqtt](https://github.com/intility/off_broadway_emqtt): [Guide](https://hexdocs.pm/off_broadway_emqtt/)
  * [off_broadway_kafka](https://github.com/bbalser/off_broadway_kafka): [Guide](https://hexdocs.pm/off_broadway_kafka/)
  * [off_broadway_memory](https://github.com/elliotekj/off_broadway_memory): [Guide](https://hexdocs.pm/off_broadway_memory/)
  * [off_broadway_redis](https://github.com/amokan/off_broadway_redis): [Guide](https://hexdocs.pm/off_broadway_redis/)
  * [off_broadway_redis_stream](https://github.com/akash-akya/off_broadway_redis_stream): [Guide](https://hexdocs.pm/off_broadway_redis_stream/)
  * [off_broadway_splunk](https://github.com/intility/off_broadway_splunk): [Guide](https://hexdocs.pm/off_broadway_splunk/)
