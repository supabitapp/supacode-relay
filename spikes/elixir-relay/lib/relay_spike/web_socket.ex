defmodule RelaySpike.WebSocket do
  @behaviour :cowboy_websocket
  @auth_timeout 5_000
  @signature_prefix "supacode-relay-v1\n"

  @impl true
  def init(req, route), do: {:cowboy_websocket, req, %{route: route, query: Map.new(:cowboy_req.parse_qs(req))}}

  @impl true
  def websocket_init(%{route: :control, query: query} = state) do
    with {:ok, pub} <- decode_exact(Map.get(query, "publicKey"), 32) do
      endpoint_id = endpoint_id(pub)
      nonce = random_b64(32)
      Process.send_after(self(), :auth_timeout, @auth_timeout)
      {:reply, {:text, Jason.encode!(%{type: "challenge", nonce: nonce})}, Map.merge(state, %{phase: :auth, pub: pub, endpoint_id: endpoint_id, nonce: nonce})}
    else
      _ -> {:reply, {:close, 1008, "invalid publicKey"}, state}
    end
  end

  def websocket_init(%{route: :connect, query: query} = state) do
    endpoint_id = Map.get(query, "endpointId", "")
    case RelaySpike.Store.new_pair(endpoint_id, self()) do
      {:ok, id, token} ->
        {:ok, Map.merge(state, %{phase: :pending, role: :client, pair_id: id, token: token, pending_frames: []})}
      {:error, :not_found} -> {:reply, {:close, 1003, "endpoint not found"}, state}
      {:error, :capacity} -> {:reply, {:close, 1013, "client capacity reached"}, state}
    end
  end

  def websocket_init(%{route: :accept, query: query} = state) do
    with endpoint_id when is_binary(endpoint_id) <- Map.get(query, "endpointId"),
         id when is_binary(id) <- Map.get(query, "connectionId"),
         token when is_binary(token) <- Map.get(query, "token"),
         :ok <- RelaySpike.Store.accept_pair(endpoint_id, id, token, self()) do
      {:ok, Map.merge(state, %{phase: :active, role: :host, pair_id: id})}
    else
      _ -> {:reply, {:close, 1008, "invalid token"}, state}
    end
  end

  @impl true
  def websocket_handle({:text, data}, %{route: :control, phase: :auth} = state) do
    case Jason.decode(data) do
      {:ok, %{"type" => "authenticate", "signature" => signature}} -> authenticate(state, signature)
      _ -> {:reply, {:close, 1008, "authentication failed"}, state}
    end
  end

  def websocket_handle(_frame, %{route: :control} = state), do: {:reply, {:close, 1008, "unexpected control message"}, state}
  def websocket_handle({type, data}, %{phase: :pending, pending_frames: frames} = state) when type in [:text, :binary] and length(frames) < 64 do
    {:ok, %{state | pending_frames: frames ++ [{type, data}]}}
  end

  def websocket_handle(_frame, %{phase: :pending} = state), do: {:ok, state}

  def websocket_handle({type, data}, %{phase: :active} = state) when type in [:text, :binary] do
    case peer(state) do
      pid when is_pid(pid) ->
        send(pid, {:relay_frame, type, data})
        RelaySpike.Store |> send({:forwarded, byte_size(data)})
        {:ok, state}

      _ -> {:reply, {:close, 1001, "peer disconnected"}, state}
    end
  end

  def websocket_handle(_frame, state) do
    {:ok, state}
  end

  @impl true
  def websocket_info(:auth_timeout, %{phase: :auth} = state), do: {:reply, {:close, 1008, "authentication timeout"}, state}
  def websocket_info(:superseded, state), do: {:reply, {:close, 4001, "registration superseded"}, state}

  def websocket_info({:incoming, id, token}, state) do
    frame = Jason.encode!(%{type: "incoming", connectionId: id, token: token})
    {:reply, {:text, frame}, state}
  end

  def websocket_info({:pair_ready, peer_pid}, state) do
    Enum.each(state.pending_frames, fn {type, data} -> send(peer_pid, {:relay_frame, type, data}) end)
    {:ok, %{state | phase: :active, pending_frames: []}}
  end
  def websocket_info({:peer_closed, code, reason}, state), do: {:reply, {:close, code, reason}, state}
  def websocket_info({:relay_frame, type, data}, state), do: {:reply, {type, data}, state}
  def websocket_info(_message, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _req, %{route: :control, endpoint_id: endpoint_id}) do
    RelaySpike.Store.remove_host(endpoint_id, self())
    :ok
  end

  def terminate(_reason, _req, %{pair_id: id}) do
    RelaySpike.Store.close_pair(id, self())
  end
  def terminate(_reason, _req, _state), do: :ok

  defp authenticate(state, signature) do
    with {:ok, sig} <- decode_exact(signature, 64),
         true <- :crypto.verify(:eddsa, :none, @signature_prefix <> state.endpoint_id <> "\n" <> state.nonce, sig, [state.pub, :ed25519]),
         :ok <- RelaySpike.Store.register(state.endpoint_id, self()) do
      {:reply, {:text, Jason.encode!(%{type: "registered", endpointId: state.endpoint_id})}, %{state | phase: :registered}}
    else
      _ -> {:reply, {:close, 1008, "authentication failed"}, state}
    end
  end

  defp peer(%{role: :client, pair_id: id}), do: pair_peer(id, :client)
  defp peer(%{role: :host, pair_id: id}), do: pair_peer(id, :host)

  defp pair_peer(id, side) do
    case :sys.get_state(RelaySpike.Store) do
      %{pairs: %{^id => pair}} -> if side == :client, do: pair.host, else: pair.client
      _ -> nil
    end
  end

  defp decode_exact(value, size) when is_binary(value) do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} when byte_size(decoded) == size -> {:ok, decoded}
      _ -> :error
    end
  end

  defp decode_exact(_, _), do: :error
  defp endpoint_id(pub), do: :crypto.hash(:sha256, pub) |> Base.encode16(case: :lower)
  defp random_b64(bytes), do: :crypto.strong_rand_bytes(bytes) |> Base.url_encode64(padding: false)
end
