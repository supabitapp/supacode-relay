defmodule RelaySpike.Store do
  use GenServer

  @pair_timeout 30_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def register(endpoint_id, control_pid), do: GenServer.call(__MODULE__, {:register, endpoint_id, control_pid})
  def remove_host(endpoint_id, control_pid), do: GenServer.call(__MODULE__, {:remove_host, endpoint_id, control_pid})
  def new_pair(endpoint_id, client_pid), do: GenServer.call(__MODULE__, {:new_pair, endpoint_id, client_pid})
  def accept_pair(endpoint_id, connection_id, token, host_pid), do: GenServer.call(__MODULE__, {:accept_pair, endpoint_id, connection_id, token, host_pid})
  def close_pair(connection_id, peer_pid), do: GenServer.call(__MODULE__, {:close_pair, connection_id, peer_pid})
  def metrics, do: GenServer.call(__MODULE__, :metrics)

  @impl true
  def init(opts) do
    {:ok, %{hosts: %{}, pairs: %{}, max_clients: Keyword.fetch!(opts, :max_clients), clients: 0, forwarded_messages: 0, forwarded_bytes: 0}}
  end

  @impl true
  def handle_call({:register, endpoint_id, control_pid}, _from, state) do
    old = Map.get(state.hosts, endpoint_id)
    if old && old.pid != control_pid, do: send(old.pid, :superseded)
    Process.monitor(control_pid)
    host = %{pid: control_pid, pending: 0, active: 0}
    {:reply, :ok, %{state | hosts: Map.put(state.hosts, endpoint_id, host)}}
  end

  def handle_call({:remove_host, endpoint_id, control_pid}, _from, state) do
    case Map.get(state.hosts, endpoint_id) do
      %{pid: ^control_pid} ->
        {pairs, kept} = Enum.split_with(state.pairs, fn {_id, pair} -> pair.endpoint_id == endpoint_id end)
        Enum.each(pairs, fn {_id, pair} -> notify_all(pair, 1001, "host offline") end)
        {:reply, :ok, %{state | hosts: Map.delete(state.hosts, endpoint_id), pairs: Map.new(kept), clients: max(state.clients - length(pairs), 0)}}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:new_pair, endpoint_id, client_pid}, _from, state) do
    case Map.get(state.hosts, endpoint_id) do
      nil -> {:reply, {:error, :not_found}, state}
      _host when state.clients >= state.max_clients -> {:reply, {:error, :capacity}, state}
      host ->
        id = random_b64(16)
        token = random_b64(32)
        pair = %{endpoint_id: endpoint_id, client: client_pid, host: nil, token: token, timer: Process.send_after(self(), {:expire, id}, @pair_timeout), state: :pending}
        send(host.pid, {:incoming, id, token})
        Process.monitor(client_pid)
        {:reply, {:ok, id, token}, %{state | pairs: Map.put(state.pairs, id, pair), clients: state.clients + 1}}
    end
  end

  def handle_call({:accept_pair, endpoint_id, id, token, host_pid}, _from, state) do
    case Map.get(state.pairs, id) do
      %{endpoint_id: ^endpoint_id, token: ^token, state: :pending, client: client_pid} = pair ->
        Process.cancel_timer(pair.timer)
        updated = %{pair | host: host_pid, state: :active}
        send(client_pid, {:pair_ready, host_pid})
        Process.monitor(host_pid)
        {:reply, :ok, %{state | pairs: Map.put(state.pairs, id, updated)}}

      _ ->
        {:reply, {:error, :invalid_token}, state}
    end
  end

  def handle_call({:close_pair, id, peer_pid}, _from, state) do
    {:reply, :ok, drop_pair(state, id, peer_pid, "peer disconnected")}
  end

  def handle_call(:metrics, _from, state) do
    active = Enum.count(state.pairs, fn {_id, pair} -> pair.state == :active end)
    {:reply, %{activeHosts: map_size(state.hosts), activePairs: active, pendingPairs: map_size(state.pairs) - active, forwardedMessages: state.forwarded_messages, forwardedBytes: state.forwarded_bytes}, state}
  end

  @impl true
  def handle_info({:expire, id}, state) do
    case Map.get(state.pairs, id) do
      %{state: :pending} = pair ->
        send(pair.client, {:peer_closed, 1013, "pair timeout"})
        {:noreply, %{state | pairs: Map.delete(state.pairs, id), clients: max(state.clients - 1, 0)}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:forwarded, bytes}, state), do: {:noreply, %{state | forwarded_messages: state.forwarded_messages + 1, forwarded_bytes: state.forwarded_bytes + bytes}}

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    host_ids = for {id, %{pid: ^pid}} <- state.hosts, do: id
    state = Enum.reduce(host_ids, state, fn id, acc -> drop_host(acc, id, pid) end)
    pair_ids = for {id, pair} <- state.pairs, pair.client == pid or pair.host == pid, do: id
    state = Enum.reduce(pair_ids, state, fn id, acc -> drop_pair(acc, id, pid, "peer disconnected") end)
    {:noreply, state}
  end

  defp drop_host(state, endpoint_id, pid) do
    case Map.get(state.hosts, endpoint_id) do
      %{pid: ^pid} ->
        Enum.each(state.pairs, fn {_id, pair} -> if pair.endpoint_id == endpoint_id, do: notify_all(pair, 1001, "host offline") end)
        pairs = Enum.reject(state.pairs, fn {_id, pair} -> pair.endpoint_id == endpoint_id end) |> Map.new()
        %{state | hosts: Map.delete(state.hosts, endpoint_id), pairs: pairs}

      _ -> state
    end
  end

  defp drop_pair(state, id, peer_pid, reason) do
    case Map.get(state.pairs, id) do
      nil -> state
      pair when pair.client == peer_pid or pair.host == peer_pid ->
        other = if pair.client == peer_pid, do: pair.host, else: pair.client
        if is_pid(other), do: send(other, {:peer_closed, 1001, reason})
        Process.cancel_timer(pair.timer)
        %{state | pairs: Map.delete(state.pairs, id), clients: max(state.clients - 1, 0)}

      _ -> state
    end
  end

  defp notify_all(pair, code, reason) do
    for pid <- [pair.client, pair.host], is_pid(pid), do: send(pid, {:peer_closed, code, reason})
  end

  defp random_b64(bytes), do: :crypto.strong_rand_bytes(bytes) |> Base.url_encode64(padding: false)
end
