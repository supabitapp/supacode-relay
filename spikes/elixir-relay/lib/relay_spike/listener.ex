defmodule RelaySpike.Listener do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    port = Keyword.fetch!(opts, :port)
    dispatch = Keyword.fetch!(opts, :dispatch)
    case :cowboy.start_clear(:http, [port: port], %{env: %{dispatch: dispatch}}) do
      {:ok, _pid} ->
        actual_port = :ranch.get_port(:http)
        IO.puts(Jason.encode!(%{event: "listening", address: "127.0.0.1:#{actual_port}"}))
        {:ok, %{listener: :http}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, %{listener: listener}), do: :cowboy.stop_listener(listener)
end
