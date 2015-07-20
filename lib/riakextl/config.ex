defmodule RiakExtl.Config do

  def init_config do
    {:ok, agent} = Agent.start_link fn -> %{} end
    Process.register(agent, :config)
  end

  def config(attr, val) do
    Agent.update(Process.whereis(:config), fn map -> Map.put(map, attr, val) end)
  end

  def config(attr) do
    Agent.get(Process.whereis(:config), fn map -> Map.get map, attr end)
  end

end
