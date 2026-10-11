defmodule RelaySpike.HTTP do
  def init(req, :health) do
    {:ok, :cowboy_req.reply(200, %{<<"content-type">> => <<"application/json">>}, Jason.encode!(%{status: "ok"}), req), :health}
  end

  def init(req, :metrics) do
    {:ok, :cowboy_req.reply(200, %{<<"content-type">> => <<"application/json">>}, Jason.encode!(RelaySpike.Store.metrics()), req), :metrics}
  end
end
