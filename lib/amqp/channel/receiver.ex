defmodule AMQP.Channel.Receiver do
  @moduledoc false

  import AMQP.Core
  alias AMQP.Channel.ReceiverManager

  @doc """
  Handles channel messages.
  """
  @spec handle_message(pid(), pid()) :: no_return
  def handle_message(chan_pid, client_pid) do
    receive do
      {:DOWN, _ref, :process, _pid, reason} ->
        send(client_pid, {:receiver_down, reason})
        ReceiverManager.unregister_receiver(chan_pid, client_pid)
        exit(reason)

      {:EXIT, _ref, reason} ->
        send(client_pid, {:receiver_exit, reason})
        ReceiverManager.unregister_receiver(chan_pid, client_pid)
        exit(reason)

      msg ->
        do_handle_message(client_pid, msg)
        handle_message(chan_pid, client_pid)
    end
  end

  # -- Confirm.register_handler

  defp do_handle_message(client_pid,
    basic_ack(delivery_tag: delivery_tag, multiple: multiple))
  do
    send(client_pid, {:basic_ack, delivery_tag, multiple})
  end

  defp do_handle_message(client_pid,
    basic_nack(delivery_tag: delivery_tag, multiple: multiple))
  do
    send(client_pid, {:basic_nack, delivery_tag, multiple})
  end

  # -- Basic.consume

  defp do_handle_message(client_pid,
    basic_consume_ok(consumer_tag: consumer_tag))
  do
    send(client_pid, {:basic_consume_ok, %{consumer_tag: consumer_tag}})
  end

  defp do_handle_message(client_pid,
    basic_cancel_ok(consumer_tag: consumer_tag))
  do
    send(client_pid, {:basic_cancel_ok, %{consumer_tag: consumer_tag}})
  end

  defp do_handle_message(client_pid,
    basic_cancel(consumer_tag: consumer_tag, nowait: no_wait))
  do
    send(client_pid, {:basic_cancel, %{consumer_tag: consumer_tag, no_wait: no_wait}})
  end

  defp do_handle_message(client_pid, {
    basic_deliver(
      consumer_tag: consumer_tag,
      delivery_tag: delivery_tag,
      redelivered: redelivered,
      exchange: exchange,
      routing_key: routing_key
    ),
    amqp_msg(
      props: p_basic(
        content_type: content_type,
        content_encoding: content_encoding,
        headers: headers,
        delivery_mode: delivery_mode,
        priority: priority,
        correlation_id: correlation_id,
        reply_to: reply_to,
        expiration: expiration,
        message_id: message_id,
        timestamp: timestamp,
        type: type,
        user_id: user_id,
        app_id: app_id,
        cluster_id: cluster_id
      ),
      payload: payload
    )})
  do
    send(client_pid, {:basic_deliver, payload, %{
      consumer_tag: consumer_tag,
      delivery_tag: delivery_tag,
      redelivered: redelivered,
      exchange: exchange,
      routing_key: routing_key,
      content_type: content_type,
      content_encoding: content_encoding,
      headers: headers,
      persistent: delivery_mode == 2,
      priority: priority,
      correlation_id: correlation_id,
      reply_to: reply_to,
      expiration: expiration,
      message_id: message_id,
      timestamp: timestamp,
      type: type,
      user_id: user_id,
      app_id: app_id,
      cluster_id: cluster_id
    }})
  end

  # -- Unhandled message
  defp do_handle_message(client_pid, maybe_error) do
    send(client_pid, maybe_error)
  end
end
