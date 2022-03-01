# Architecture

Broadway's architecture is built on top of GenStage. That means we structure
our processing units as independent stages that are responsible for one
individual task in the pipeline. By implementing the `Broadway` behaviour,
we define a `GenServer` process that wraps a `Supervisor` to manage and
own our pipeline.

## The pipeline model

```asciidoc
                                       [producers]   <- pulls data from SQS, RabbitMQ, etc.
                                            |
                                            |   (demand dispatcher)
                                            |
   handle_message/3 and  ---------->   [processors]
   prepare_messages/2 run here             / \
                                          /   \   (partition dispatcher)
                                         /     \
                                   [batcher]   [batcher]   <- one for each batcher key
                                       |           |
                                       |           |   (demand dispatcher)
                                       |           |
handle_batch/4 runs here -> [batch processor][batch processor]
```

## Internal stages

  * `Broadway.Producer` - A wrapper around the actual producer defined by
    the user. It serves as the source of the pipeline.
  * `Broadway.Processor` - This is where messages are processed, e.g. do
    calculations, convert data into a custom json format etc. Here is where
    the code from `handle_message/3` runs.
  * `Broadway.Batcher` - Creates batches of messages based on the
    batcher's key. One batcher for each key will be created.
  * `Broadway.BatchProcessor` - This is where the code from `handle_batch/4` runs.

## The supervision tree

Broadway was designed to always go back to a working state in case
of failures thanks to the use of supervisors. Our supervision tree
is designed as follows:

```asciidoc
                                   [Broadway GenServer]
                                            |
                                            |
                                            |
                              [Broadway Pipeline Supervisor]
                             /    /   (:rest_for_one)   \    \
                           /     |                       |      \
                         /       |                       |         \
                       /         |                       |            \
                     /           |                       |               \
                   /             |                       |                  \
  [ProducerSupervisor]  [ProcessorSupervisor]   [BatchersSupervisor]    [Terminator]
    (:one_for_one)        (:one_for_all)           (:one_for_one)
         / \                    / \                /            \
        /   \                  /   \              /              \
       /     \                /     \            /                \
      /       \              /       \          /                  \
[Producer_1]  ...    [Processor_1]  ...  [BatcherSupervisor_1]     ...
                                            (:rest_for_one)
                                            /             \
                                           /               \
                                          /                 \
                                     [Batcher]   [BatchProcessorSupervisor]
                                                       (:one_for_all)
                                                        /           \
                                                       /             \
                                                      /               \
                                            [BatchProcessor_1]        ...
```


Both `ProcessorSupervisor` and `BatchProcessorSupervisor` are set with
`max_restarts` to 0. The idea is that if any process fails, we want
to restart the rest of the tree. Since Broadway callbacks are
stateless, we can handle errors and provide reports without crashing
processes. This means that the supervision tree will only shutdown
in case of unforeseen errors in Broadway's implementation.

The only exception are the producers, which contain external code
and are expected to fail. If a producer crashes, it will be restarted
by its supervisor without cascading failures until its max restarts
is reached. Broadway automatically handles those failures by making
processors automatically resubscribe to producers in case of crashes.

## Graceful shutdowns

The cascading failures aspect also provides safe semantics for graceful
shutdown. We know that either all processes are running OR they are all
being shutdown. Therefore, to gracefully shutdown the supervision tree,
a terminator process is activated, which starts the following steps:

  1. It notifies the first layer of processors that they should not
     resubscribe to producers once they exit

  2. It tells all producers to no longer accept demand, flush all
     current events, and then shutdown

  3. It then monitors and waits for a confirmation message from batch
     processors. At this point, the terminator is effectively blocking
     the supervisor until all events have been processed

This triggers a cascade effect where processors notice all of its producers
have been cancelled, causing them to flush their own events and cancels the
stages downstream, and so on and so on. This happens until batch processors
notice all of their producers have been cancelled, effectively notifying the
terminator to shutdown, allowing the outer most supervisor to go on and fully
terminate all stages, which at this point have flushed all events.
