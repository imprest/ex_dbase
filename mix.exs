defmodule ExDbase.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_dbase,
      version: "1.0.0",
      elixir: "~> 1.14",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      name: "ExDbase",
      source_url: "https://github.com/imprest/ex_dbase",
      docs: [
        main: "ExDbase"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    []
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:decimal, "~> 2.0"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:styler, "~> 0.11.9", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "Elixir lib to parse dBASE III (.dbf) files"
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/imprest/ex_dbase"}
    ]
  end
end
