# Google Cloud Pub/Sub

Cloud Pub/Sub is a fully-managed real-time messaging service provided by Google.

## Getting Started

In order to use Broadway with Cloud Pub/Sub we need to:

  1. Setup Cloud Pub/Sub project
  1. Configure our Elixir project to use Broadway
  1. Define your pipeline configuration
  1. Implement Broadway callbacks
  1. Run the Broadway pipeline
  1. Tuning the configuration (Optional)

If you are just getting familiar with Google Pub/Sub, refer to [the documentation](https://cloud.google.com/pubsub/docs/)
to get started. Instead of testing against a live environemnt, you may also consider using the
[emulator](https://cloud.google.com/pubsub/docs/emulator) to simulate integrating with Cloud
Pub/Sub.

If you have an existing project, topic, subcription, and credentials, you can skip [step
1](#setup-cloud-pub-sub-project) and jump to [Configure the project](#configure-the-project)
section.

## Setup Cloud Pub/Sub project

In this tutorial we'll use the [`gcloud`](https://cloud.google.com/sdk/gcloud/) command-line tool
to set everything up in Google Cloud. Alternatively, you can roughly follow this guide by using
[Cloud Console](https://console.cloud.google.com).

To install `gcloud` follow the [documentation](https://cloud.google.com/sdk/gcloud/). If you are
on macOS you may consider installing it with Homebrew:

    $ brew cask install google-cloud-sdk

Now, authenticate the CLI:

    $ gcloud auth login

Then, create a new project:

    $ gcloud projects create test-pubsub

A new topic:

    $ gcloud pubsub topics create test-topic --project test-pubsub
    Created topic [projects/test-pubsub/topics/test-topic].

And a new subscription:

    $ gcloud pubsub subscriptions create test-subscription --project test-pubsub --topic test-topic
    Created subscription [projects/test-pubsub/subscriptions/test-subscription].

We also need a [service account](https://cloud.google.com/iam/docs/service-accounts), an IAM
policy, as well as API credentials in order to programatically work with the service. First, let's
create the service account:

    $ gcloud iam service-accounts create test-account --project test-pubsub
    Created service account [test-account].

Then the policy. For simplicity we add the genreal role `roles/editor`, but make

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

In this guide we're going to use [BroadwayCloudPubSub](https://github.com/plataformatec/broadway_cloud_pub_sub),
which is a Broadway Cloud Pub/Sub Connector provided by [Plataformatec](http://www.plataformatec.com).

### Starting a new project

If you plan to start a new project, just run:

    mix new my_app --sup

The `--sup` flag instructs Elixir to generate an application with a supervision tree.

### Setting up dependencies

Add `:broadway_cloud_pub_sub` to the list of dependencies in `mix.exs`, along with the Google
Cloud authentication library of your choice (defaults to `:goth`):
`:hackney`):

    defp deps() do
      [
        ...
        {:broadway_cloud_pub_sub, "~> 0.4.0"},
        {:goth, "~> 1.0"}
      ]
    end

Don't forget to check for the latest version of dependencies.

## Define the pipeline configuration

Broadway is a process-based behaviour and to define a Broadway pipeline, we need to define three
functions: `start_link/1`, `handle_message/3` and `handle_batch/4`. We will cover `start_link/1`
in this section and the `handle_` callbacks in the next one.

Similar to other process-based behaviour, `start_link/1` simply delegates to
`Broadway.start_link/2`, which should define the producers, processors, and batchers in the
Broadway pipeline. Assuming we want to consume messages from the `test-subscription`, the minimal
configuration would be:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producers: [
            default: [
              module:
                {BroadwayCloudPubSub.Producer,
                 subscription: "projects/test-pubsub/subscriptions/test-subscription"}
            ]
          ],
          processors: [
            default: []
          ],
          batchers: [
            default: [
              batch_size: 10,
              batch_timeout: 2000
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

> Note: Even though batching is optional in Broadway v0.2, we recommend all Cloud Pub/Sub
> pipelines to have at least a default batcher, with the default values defined above, unless you
> are expecting a very low rate of incoming messages. That's because batchers will also
> acknowledge messages in batches, which is the most cost and time efficient way of doing so on
> Cloud Pub/Sub.

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
applications have a supervision tree defined at `lib/my_app/application.ex`. You can add Broadway
as a child to a supervisor as follows:

    children = [
      {MyBroadway, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

The final step is to configure credentials. You can set the following environment variable:

    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

See [Goth](https://github.com/peburrows/goth) documentation for alternative ways of authenticating
with the API.

Now the Broadway pipeline should be started when your application starts. Also, if your Broadway
pipeline has any dependency (for example, it needs to talk to the database), make sure that
it is listed *after* its dependencies in the supervision tree.

If you followed the previous section about setting the project with `gcloud`, you can now test the
the pipeline. In one terminal tab start the application:

    $ iex -S mix

And in another tab, send a couple of test messages to Pub/Sub:

    $ gcloud pubsub topics publish  projects/test-pubsub/topics/test-topic --message "test 1"
    messageIds:
    - '651428033718119'
    $ gcloud pubsub topics publish  projects/test-pubsub/topics/test-topic --message "test 2"
    messageIds:
    - '651427034966696'

Now, In the first tab, you should see output similar to:

    Got batch of finished jobs from processors, sending ACKs to Pub/Sub as a batch.: ["TEST 1", "TEST 2"]

## Tuning the configuration

Some of the configuration options available for Broadway come already with a "reasonable" default
value. However those values might not suit your requirements. Depending on the number of messages
you get, how much processing they need and how much IO work is going to take place, you might need
completely different values to optimize the flow of your pipeline. The `stages` option available
for every set of producers, processors and batchers, among with `batch_size` and `batch_timeout`
can give you a great deal of flexibility. The `stages` option controls the concurrency level in
each layer of the pipeline. Here's an example on how you could tune them according to your needs.

    defmodule MyBroadway do
      use Broadway

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producers: [
            default: [
              ...
              stages: 60,
            ]
          ],
          processors: [
            default: [
              stages: 100,
            ]
          ],
          batchers: [
            default: [
              batch_size: 10,
              stages: 80,
            ]
          ]
        )
      end

      ...callbacks...
    end

In order to get a good set of configurations for your pipeline, it's important to respect the
limitations of the servers you're running, as well as the limitations of the services you're
providing/consuming data to/from.
