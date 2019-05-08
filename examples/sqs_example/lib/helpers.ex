defmodule BroadwaySQSExample.Helpers do
  def send_strings_sqs(queue, msg, amount) do
    sqs_req = ExAws.SQS.send_message(queue, msg)

    Enum.each(1..amount, fn _x ->
      Task.async(fn ->
        ExAws.request(sqs_req, region: "us-east-2")
      end)
    end)
  end

  def send_ints_sqs(queue, amount) do
    Enum.each(1..amount, fn x ->
      sqs_req = ExAws.SQS.send_message(queue, x)

      Task.async(fn ->
        ExAws.request(sqs_req, region: "us-east-2")
      end)
    end)
  end

  def create_sqs_queue(queue) do
    sqs_req = ExAws.SQS.create_queue(queue)
    ExAws.request(sqs_req, region: "us-east-2")
  end

  def create_default_queues() do
    string_queue = Application.get_env(:broadway_sqs_example, :string_queue)
    IO.inspect("creating string_queue with name: #{string_queue}")
    create_sqs_queue(string_queue)

    int_queue = Application.get_env(:broadway_sqs_example, :int_queue)
    IO.inspect("creating int_queue with name: #{int_queue}")
    create_sqs_queue(int_queue)
  end

  def send_ints() do
    int_queue = Application.get_env(:broadway_sqs_example, :int_queue)
    send_ints_sqs(int_queue, 100)
  end

  def send_strings() do
    string_queue = Application.get_env(:broadway_sqs_example, :string_queue)
    send_strings_sqs(string_queue, "testing", 100)
  end
end
