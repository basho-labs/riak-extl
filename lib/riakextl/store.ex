defmodule RiakExtl.Store do
  use GenServer

  require Logger
  import RiakExtl.Config
  import RiakExtl.Util

  def init({:riak, target, {ip, port}}) do
    Process.register(self(), target)
    {:ok, pid} = Riak.Connection.start_link(ip, port)
    Logger.debug "Connection successful"
    {:ok, %{type: :riak, pid: pid, id: target}}
  end

  def init({:file, target, {dir}}) do
    Process.register(self(), target)
    Logger.debug "File Connection Opened"
    {:ok, pid} = GenServer.start_link(RiakExtl.Store.File, {dir})
    {:ok, %{type: :file, pid: pid}}
  end

  def start_riak( target, ip, port ) when is_atom(target) do
    Logger.debug "Starting Store Server [#{to_string(target)}]"
    GenServer.start_link(__MODULE__, {:riak, target, {ip, port}})
    :ok
  end

  def start_file( target, dir ) when is_atom(target) do
    Logger.debug "Starting FS Server [#{to_string(target)}]"
    GenServer.start_link(__MODULE__, {:file, target, {dir}})
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

  def handle_call({:storeop, {mod, fun, args}}, _from, %{type: :riak, pid: pid} = state) do
    res = apply(mod, fun, [pid|args])
    {:reply, res, state}
  end

  def handle_call({:storeop, {mod, fun, args}}, _from, %{type: :file, pid: pid} = state) do
    res = RiakExtl.Store.File.run(pid,mod, fun,args)
    {:reply, res, state}
  end

  def handle_call(unknown, _from, state) do
    IO.puts("Unknown call!")
    IO.inputs(unknown)
    {:reply, :ok, state}
  end

  def stop( target ) when is_atom(target) do
    GenServer.call(riak_pid(target), {:storeop, {Riak.Connection, :stop,[]}})
    Logger.debug "Stopping Store Sever [#{to_str(target)}]"
    
  end

  def get_obj!( target, type, bucket, key ) when is_atom(target) do
    GenServer.call(riak_pid(target), {:storeop, {Riak, :find,[ { type, bucket }, key]}})
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
    GenServer.call(riak_pid(target), {:storeop, {Riak, :put, [obj]}}, 100000)
  end

  def del_obj!( target, obj ) when is_atom(target) do
    case config :op do
      false ->
        Logger.debug "NOOP [DEL]:\t #{obj.key}"
      true ->
        Logger.debug "DEL:\t #{obj.key}"
        GenServer.call(riak_pid(target), {:storeop, {Riak, :delete, [obj]}})
    end
  end

  def get_keys!( target, type, bucket ) when is_atom(target) do
    Logger.debug "Retrieving list of keys from #{type}/#{bucket} on #{to_string(target)}"
    GenServer.call(riak_pid(target), {:storeop, {Riak.Bucket, :keys!, [{type, bucket}]}})
  end

  def get_buckets!( target, type ) when is_atom (target) do
    GenServer.call(riak_pid(target), {:storeop, {Riak.Bucket.Type, :list!,[type]}})
  end

  def get_bucket( target, namespace ) do
    GenServer.call(riak_pid(target), {:storeop, {Riak.Bucket, :get, [namespace]}})
  end

  def put_bucket( target, namespace, p ) do
    GenServer.call(riak_pid(target), {:storeop, {Riak.Bucket, :put, [namespace, p]}})
  end

  def riak_ping( target ) when is_atom (target) do
    GenServer.call(target, {:storeop, {Riak, :ping,[]}})
  end


  def riak_get_index(target, index) do
    GenServer.call(riak_pid(target), {:storeop, {Riak.Search.Index, :get, [index]}})
  end

  def put_schema(target, schema, schema_xml) do
    if config :op do
      Logger.info "SCHEMA\tCREATE\tCreating Schema: #{schema}"
      GenServer.call(riak_pid(target), {:storeop, {Riak.Search.Schema, :create, [schema, schema_xml]}})
    else
      Logger.info "SCHEMA\tCREATE[NOOP]\tWould have created schema: #{schema}"
    end
  end

  def riak_index_create(target, idx, schema, props) do
    if config :op do
      Logger.info "INDEX\tCREATE\tCreating Index: #{idx[:index]}"
      GenServer.call(riak_pid(target), {:storeop, {Riak.Search.Index, :put, [idx[:index], schema, props]}}, 100000)
    else
      Logger.info "INDEX\tCREATE[NOOP]\tWould have created Index: #{idx[:index]}"
    end
  end

  def riak_get_schema( target, schema ) do
    GenServer.call(riak_pid(target), {:storeop, {Riak.Search.Schema, :get, [schema]}})
  end

end
