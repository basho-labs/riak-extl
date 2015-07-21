defmodule RiakExtl.Store do
  use GenServer

  require Logger
  import RiakExtl.Config

  def init({:riak, target, {ip, port}}) do
    Process.register(self(), target)
    {:ok, pid} = Riak.Connection.start_link(ip, port)
    Logger.debug "Connection successful"
    {:ok, %{type: :riak, pid: pid, id: target}}
  end

  def init({:file, target, {_filename}}) do
    Process.register(self(), target)
    Logger.debug "File Connection Opened"
    {:ok, %{type: :file, pid: self()}}
  end

  def start_riak( target, ip, port ) when is_atom(target) do
    Logger.debug "Starting Store Server [#{to_string(target)}]"
    GenServer.start_link(__MODULE__, {:riak, target, {ip, port}})
    :ok
  end

  def riak_pid(target) when is_atom (target) do
    pid = Process.whereis(target)
    cond do
      is_nil pid ->
        raise "Riak Connection to #{to_string target} Broken"
        :nil
      !Process.alive? pid ->
        raise "Riak Connection to #{to_string target} Broken"
        :nil
      true ->
        pid
    end
  end

  def handle_call({:storeop, {fun, args}}, _from, state) do
    Logger.debug("Storeop called")
    res = apply(fun,[state[:pid]|args])
    {:reply, res, state}
  end

  def handle_call(unknown, _from, state) do
    IO.puts("Unknown call!")
    IO.inputs(unknown)
    {:reply, :ok, state}
  end

  def get_obj!( target, type, bucket, key ) when is_atom(target) do
    GenServer.call(riak_pid(target), {:storeop, {&Riak.find/3,[ { type, bucket }, key]}})
  end

  def put_obj!( target, obj ) do
    put_obj!(target, obj, config(:op), config(:json))
  end

  def put_obj!( target, obj, op, true ) when is_atom(target) do
    case JSON.decode(obj.data) do
      {:ok, _ } -> :ok
        put_obj!( target, obj, op, false )
      {:error, :unexpected_end_of_buffer } ->
        Logger.warn "PUT ERROR\tInvalid JSON: Unexpected End of Buffer for key #{obj.key}"
      {:error, {:unexpected_token, token }} ->
        Logger.warn "PUT ERROR\tInvalid JSON: Unexpected Token: #{token} for key #{obj.key}"
    end
  end

  def put_obj!( target, obj, false, false ) when is_atom(target) do
    Logger.debug "NOOP [PUT]:\t #{obj.key}"
  end

  def put_obj!( target, obj, true, false ) when is_atom(target) do
    Logger.debug "PUT:\t #{obj.key}"
    GenServer.call(riak_pid(target), {:storeop, {&Riak.put/2, [obj]}})
  end

  def del_obj!( target, obj ) when is_atom(target) do
    case config :op do
      false ->
        Logger.debug "NOOP [DEL]:\t #{obj.key}"
      true ->
        Logger.debug "DEL:\t #{obj.key}"
        GenServer.call(riak_pid(target), {:storeop, {&Riak.delete/2, [obj]}})
    end
  end

  def get_keys!( target, type, bucket ) when is_atom(target) do
    Logger.debug "Retrieving list of keys from #{type}/#{bucket} on #{to_string(target)}"
    GenServer.call(riak_pid(target), {:storeop, {&Riak.Bucket.keys!/2, [{type, bucket}]}})
  end

  def get_buckets!( target, type ) when is_atom (target) do
    GenServer.call(riak_pid(target), {:storeop, {&Riak.Bucket.Type.list!/2,[type]}})
  end

  def get_bucket( target, namespace ) do
    GenServer.call(riak_pid(target), {:storeop, {&Riak.Bucket.get/2, [namespace]}})
  end

  def put_bucket( target, namespace, p ) do
    GenServer.call(riak_pid(target), {:storeop, {&Riak.Bucket.put/3, [namespace, p]}})
  end

  def riak_ping( target ) when is_atom (target) do
    GenServer.call(target, {:storeop, {&Riak.ping(&1),[]}})
  end


  def riak_get_index(target, index) do
    GenServer.call(riak_pid(target), {:storeop, {&Riak.Search.Index.get/2, [index]}})
  end

  def put_schema(target, schema, schema_xml) do
    if config :op do
      Logger.info "SCHEMA\tCREATE\tCreating Schema: #{schema}"
      GenServer.call(riak_pid(target), {:storeop, {&Riak.Search.Schema.create/3, [schema, schema_xml]}})
    else
      Logger.info "SCHEMA\tCREATE[NOOP]\tWould have created schema: #{schema}"
    end
  end

  def riak_index_create(target, idx, schema, props) do
    if config :op do
      Logger.info "INDEX\tCREATE\tCreating Index: #{idx[:index]}"
      GenServer.call(riak_pid(target), {:storeop, {&Riak.Search.Index.put/4, [idx[:index], schema, props]}})
    else
      Logger.info "INDEX\tCREATE[NOOP]\tWould have created Index: #{idx[:index]}"
    end
  end

  def riak_get_schema( target, schema ) do
    GenServer.call(riak_pid(target), {:storeop, {&Riak.Search.Schema.get/2, [schema]}})
  end

end
