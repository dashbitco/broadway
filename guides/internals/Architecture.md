# Architecture

TODO.

## General Architecture

Broadway's architecture is built on top GenStage. That means we structure
our processing units as independent stages that are responsible for one
individual task in the pipeline. By implementing the `Broadway` behaviour
we define a `GenServer` process that wraps a `Supervisor` to manage and
own our pipeline.

### The Pipeline Model

```asciidoc
                                   [producers]   <- pulls data from SQS, RabbitMQ, etc.
                                        |
                                        |   (demand dispatcher)
                                        |
   handle_message/3 runs here ->   [processors]
                                       / \
                                      /   \   (partition dispatcher)
                                     /     \
                               [batcher]   [batcher]   <- one for each batcher key
                                   |           |
                                   |           |   (demand dispatcher)
                                   |           |
handle_batch/4 runs here ->   [consumers]  [consumers]
```

### Internal Stages

  * `Broadway.Producer` - A wrapper around the actual producer defined by
    the user. It serves as the source of the pipeline.
  * `Broadway.Processor` - This is where messages are processed, e.g. do
    calculations, convert data into a custom json format etc. Here is where
    the code from `handle_message/3` runs.
  * `Broadway.Batcher` - Creates batches of messages based on the
    batcher's key. One Batcher for each key will be created.
  * `Broadway.Consumer` - This is where the code from `handle_batch/4` runs.

### Fault-tolerance

Broadway was designed always go back to a working state in case
of failures thanks to the use of supervisors. Our supervision tree
is designed as follows:

```asciidoc
                        [Broadway GenServer]
                                 |
                                 |
                                 |
                  [Broadway Pipeline Supervisor]
                      /   (:rest_for_one)     \
                     /           |             \
                    /            |              \
                   /             |               \
                  /              |                \
                 /               |                 \
  [ProducerSupervisor]  [ProcessorSupervisor] [BatcherPartitionSupervisor] [Terminator]
    (:one_for_one)        (:one_for_all)           (:one_for_one)
         / \                    / \                /            \
        /   \                  /   \              /              \
       /     \                /     \            /                \
      /       \              /       \          /                  \
[Producer_1]  ...    [Processor_1]  ...  [BatcherConsumerSuperv_1]  ...
                                            (:rest_for_one)
                                                  /  \
                                                 /    \
                                                /      \
                                          [Batcher] [Supervisor]
                                                    (:one_for_all)
                                                          |
                                                    [Consumer_1]
```

Another part of Broadway fault-tolerance comes from the fact the
callbacks are stateless, which allows us to provide back-off in
processors and batchers out of the box.

Finally, Broadway guarantees proper shutdown of the supervision
tree, making sure that processes only terminate after all of the
currently fetched messages are processed and published (consumer).