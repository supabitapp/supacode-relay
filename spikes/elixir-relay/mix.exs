defmodule RelaySpike.MixProject do
  use Mix.Project

  def project do
    [
      app: :relay_spike,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: [relay_spike: [include_executables_for: [:unix]]]
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger],
      mod: {RelaySpike.Application, []}
    ]
  end

  defp deps do
    [
      {:cowboy, "~> 2.13"},
      {:jason, "~> 1.4"}
    ]
  end
end
