# Google Cloud Pub/Sub

Cloud Pub/Sub is a fully-managed real-time messaging service provided by Google.

## Getting started

In order to use Broadway with Cloud Pub/Sub you need to:

  1. Setup a Cloud Pub/Sub project
  1. Configure your Elixir project to use Broadway
  1. Define your pipeline configuration
  1. Implement Broadway callbacks
  1. Run the Broadway pipeline
  1. Tune the configuration (Optional)

If you are getting familiar with Google Pub/Sub, refer to [the documentation](https://cloud.google.com/pubsub/docs/)
to get started. Instead of testing against a live environment, you may also consider using the
[emulator](https://cloud.google.com/pubsub/docs/emulator) to simulate integrating with Cloud
Pub/Sub.

Existing projects, topics, subscriptions, and credentials can skip [step
1](#setup-cloud-pub-sub-project) and jump to [Configure the project](#configure-the-project)
section.

## Setup Cloud Pub/Sub project

In this tutorial we'll use the [`gcloud`](https://cloud.google.com/sdk/gcloud/) command-line tool
to set everything up in Google Cloud. Alternatively, follow this
[Cloud Console](https://console.cloud.google.com).

To install `gcloud` follow the [documentation](https://cloud.google.com/sdk/gcloud/). If you are
on macOS you may consider installing it with Homebrew:

    $ brew install --cask google-cloud-sdk

Now, authenticate the CLI:

    $ gcloud auth login

Then, create a new project:

    $ gcloud projects create test-pubsub

A new topic:

    $ gcloud pubsub topics create test-topic --project test-pubsub
    Created topic [projects/test-pubsub/topics/test-topic].

> Note: If an error indicates the organization policy is still provisioning after creating a new Google Cloud project, wait a couple minutes and try again.

And a new subscription:

    $ gcloud pubsub subscriptions create test-subscription --project test-pubsub --topic test-topic
    Created subscription [projects/test-pubsub/subscriptions/test-subscription].

We also need a [service account](https://cloud.google.com/iam/docs/service-accounts), an IAM
policy, as well as API credentials in order to programmatically work with the service. First, let's
create the service account:

    $ gcloud iam service-accounts create test-account --project test-pubsub
    Created service account [test-account].

Then the policy. For simplicity we add the general role `roles/editor`, but make sure to
examine the [available roles](https://cloud.google.com/iam/docs/understanding-roles#pubsub-roles)
and choose the one that best suits your use case:

    $ gcloud projects add-iam-policy-binding test-pubsub \
        --member serviceAccount:test-account@test-pubsub.iam.gserviceaccount.com \
        --role roles/editor
    Updated IAM policy for project [test-pubsub].
    (...)

And now the credentials:

    $ gcloud iam service-accounts keys create credentials.json --iam-account=test-account@test-pubsub.iam.gserviceaccount.com
    created key [xxx] of type [json] as [key] for [test-account@test-pubsub.iam.gserviceaccount.com]

This command generated a `credentials.json` file which will be useful later. Note, the IAM account
pattern is `<account>@<project>.iam.gserviceaccount.com`. Run `gcloud iam service-accounts list --project test-pubsub`
to see all service accounts associated with the given project.

Finally, we need to enable Pub/Sub for our project:

    $ gcloud services enable pubsub --project test-pubsub
    Operation "operations/xxx" finished successfully.

## Configure the project

In this guide we're going to use [BroadwayCloudPubSub](https://github.com/dashbitco/broadway_cloud_pub_sub),
which is a Broadway Cloud Pub/Sub Connector provided by [Dashbit](https://dashbit.co/).

### Starting a new project

If you plan to start a new project, run:

    $ mix new my_app --sup

The `--sup` flag instructs Elixir to generate an application with a supervision tree.

### Setting up dependencies

Add `:broadway_cloud_pub_sub` to the list of dependencies in `mix.exs`, along with the Google
Cloud authentication library of your choice (defaults to `:goth`):

    defp deps() do
      [
        ...
        {:broadway_cloud_pub_sub, "~> 0.7"},
        {:goth, "~> 1.0"}
      ]
    end

Don't forget to check for the latest version of dependencies.

## Define the pipeline configuration

Broadway is a process-based behaviour and to define a Broadway pipeline, we need to define three
functions: `start_link/1`, `handle_message/3` and `handle_batch/4`. We will cover `start_link/1`
in this section and the `handle_` callbacks in the next one.

Similar to other process-based behaviour, `start_link/1` delegates to
`Broadway.start_link/2`, which should define the producers, processors, and batchers in the
Broadway pipeline. Assuming we want to consume messages from the `test-subscription`, the minimal
configuration would be:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [
            module:
              {BroadwayCloudPubSub.Producer,
               subscription: "projects/test-pubsub/subscriptions/test-subscription"}
          ],
          processors: [
            default: []
          ],
          batchers: [
            default: [
              batch_size: 10,
              batch_timeout: 2_000
            ]
          ]
        )
      end

      ...callbacks...
    end

For a full list of options for `BroadwayCloudPubSub.Producer`, please see [the
documentation](https://hexdocs.pm/broadway_cloud_pub_sub).

For general information about setting up Broadway, see `Broadway` module docs as well as
`Broadway.start_link/2`.

> Note: Even though batching is optional since Broadway v0.2, we recommend all Cloud Pub/Sub
> pipelines to have at least a default batcher, as that allows you to control the exact batch
> size and frequency that messages are acknowledged to Cloud Pub/Sub, which often leads to
> pipelines that are more cost and time efficient.

## Implement Broadway callbacks

In order to process incoming messages, we need to implement the required callbacks. For the sake
of simplicity, we're considering that all messages received from the queue are strings and our
processor calls `String.upcase/1` on them:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      ...start_link...

      def handle_message(_, %Message{data: data} = message, _) do
        message
        |> Message.update_data(fn data -> String.upcase(data) end)
      end

      def handle_batch(_, messages, _, _) do
        list = messages |> Enum.map(fn e -> e.data end)
        IO.inspect(list, label: "Got batch of finished jobs from processors, sending ACKs to Pub/Sub as a batch.")
        messages
      end
    end

We are not doing anything fancy here, but it should be enough for our purpose. First we update the
message's data individually inside `handle_message/3` and then we print each batch inside
`handle_batch/4`.

For more information, see `c:Broadway.handle_message/3` and `c:Broadway.handle_batch/4`.

## Run the Broadway pipeline

To run your `Broadway` pipeline, you need to add it as a child in a supervision tree. Most
applications have a supervision tree defined at `lib/my_app/application.ex`. Add Broadway
as a child to a supervisor as follows:

    children = [
      {MyBroadway, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

The final step is to configure credentials. Set the following environment variable:

    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

See [Goth](https://github.com/peburrows/goth) documentation for alternative ways of authenticating
with the API.

Now the Broadway pipeline should be started when your application starts. Also, if your Broadway
pipeline has any dependency (for example, it needs to talk to the database), make sure that
it is listed *after* its dependencies in the supervision tree.

In the previous section `gcloud` set up the project. Now test the
the pipeline. In one terminal tab start the application:

    $ iex -S mix

And in another tab, send a couple of test messages to Pub/Sub:

    $ gcloud pubsub topics publish  projects/test-pubsub/topics/test-topic --message "test 1"
    messageIds:
    - '651428033718119'

    gcloud pubsub topics publish  projects/test-pubsub/topics/test-topic --message "test 2"
    messageIds:
    - '651427034966696'

Now, In the first tab, you should see output similar to:

```
Got batch of finished jobs from processors, sending ACKs to Pub/Sub as a batch.: ["TEST 1", "TEST 2"]
```

## Tuning the configuration

Some of the configuration options available for Broadway come already with a
"reasonable" default value. However those values might not suit your
requirements. Depending on the number of messages you get, how much processing
they need and how much IO work is going to take place, you might need completely
different values to optimize the flow of your pipeline. The `concurrency` option
available for every set of producers, processors and batchers, among with
`max_demand`, `batch_size`, and `batch_timeout` can give you a great deal
of flexibility.

The `concurrency` option controls the concurrency level in each layer of
the pipeline.
See the notes on [`Producer concurrency`](https://hexdocs.pm/broadway/Broadway.html#module-producer-concurrency)
and [`Batcher concurrency`](https://hexdocs.pm/broadway/Broadway.html#module-batcher-concurrency)
for details.

Here's an example on how you could tune them according to
your needs.

    defmodule MyBroadway do
      use Broadway

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [
            ...
            concurrency: 10,
          ],
          processors: [
            default: [
              concurrency: 100,
              max_demand: 1,
            ]
          ],
          batchers: [
            default: [
              batch_size: 10,
              concurrency: 10,
            ]
          ]
        )
      end

      ...callbacks...
    end

In order to get a good set of configurations for your pipeline, it's
important to respect the limitations of the servers you're running,
as well as the limitations of the services you're providing/consuming
data to/from. Broadway comes with telemetry, so you can measure your
pipeline and help ensure your changes are effective.
