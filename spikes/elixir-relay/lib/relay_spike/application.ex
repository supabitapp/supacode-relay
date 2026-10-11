defmodule RelaySpike.Application do
  use Application

  def start(_type, _args) do
    port = env_integer("RELAY_PORT", 8080)
    dispatch = :cowboy_router.compile([{:_, [
      {"/healthz", RelaySpike.HTTP, :health},
      {"/metrics", RelaySpike.HTTP, :metrics},
      {"/v1/control", RelaySpike.WebSocket, :control},
      {"/v1/connect", RelaySpike.WebSocket, :connect},
      {"/v1/accept", RelaySpike.WebSocket, :accept}
    ]}])

    children = [
      {RelaySpike.Store, [max_clients: env_integer("RELAY_MAX_CLIENTS", 100_000)]},
      {RelaySpike.Listener, [port: port, dispatch: dispatch]}
    ]

    case Supervisor.start_link(children, strategy: :one_for_one, name: RelaySpike.Supervisor) do
      {:ok, _pid} = result -> result

      other ->
        other
    end
  end

  defp env_integer(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end
