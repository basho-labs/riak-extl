defmodule RiakExtl.Config do
  def merge_ini(path) do
    if File.exists?(path) do
      {:ok, iniConfig} = File.read path
      parsedIni = parseIni(iniConfig)
      Enum.map(parsedIni, fn({k,v}) -> Application.put_env(:riakextl, k,v) end)
    end
  end

  def config(attr, val) do
    Application.put_env(:riakextl, attr, val)
  end

  def config(attr) do
    Application.get_env(:riakextl, attr)
  end

  defp parseIni(data) do
    String.split(data, "\n")
    |> Enum.filter(&String.contains?(&1, "="))
    |> Enum.map(
      fn line ->
        [key, value] = String.split(line, "=")
        key = String.strip(key) |> String.to_atom
        convertAttr(key, String.strip(value))
      end)
  end

  defp convertAttr(attr, val) do
      attrStr = Atom.to_string(attr)
      case String.ends_with?(attrStr, "_port") do
          true -> {attr, String.to_integer(val)}
          _ -> {attr,val}
      end
  end
end
