defmodule RiakExtl.Store do
  require Logger
  import RiakExtl.Config

  def start_riak( target, ip, port ) when is_atom(target) do
    Logger.debug "Connecting to Riak [#{to_string(target)}]"
    { :ok, riak } = Riak.Connection.start(ip, port)
    Process.register(riak, target)
    Logger.debug "Connection successful"
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

  def get_obj!( target, type, bucket, key ) when is_atom(target) do
    Riak.find(riak_pid(target), { type, bucket }, key)
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
    Riak.put(riak_pid(target), obj)
  end

  def del_obj!( target, obj ) when is_atom(target) do
    case config :op do
      false ->
        Logger.debug "NOOP [DEL]:\t #{obj.key}"
      true ->
        Logger.debug "DEL:\t #{obj.key}"
        Riak.delete(riak_pid(target), obj)
    end
  end

  def get_keys!( target, type, bucket ) when is_atom(target) do
    Logger.debug "Retrieving list of keys from #{type}/#{bucket} on #{to_string(target)}"
    Riak.Bucket.keys!(riak_pid(target), {type, bucket})
  end

  def get_buckets!( target, type ) when is_atom (target) do
    Riak.Bucket.Type.list!(riak_pid(target), type)
  end

  def get_bucket( target, namespace ) do
    Riak.Bucket.get(riak_pid(target), namespace)
  end

  def put_bucket( target, namespace, p ) do
    Riak.Bucket.put(riak_pid(target), namespace, p)
  end

  def riak_ping( target ) when is_atom (target) do
    Riak.ping(riak_pid(target))
  end


  def riak_get_index(target, index) do
    Riak.Search.Index.get(riak_pid(target), index)
  end

  def put_schema(target, schema, schema_xml) do
    if config :op do
      Logger.info "SCHEMA\tCREATE\tCreating Schema: #{schema}"
      Riak.Search.Schema.create(riak_pid(pid), schema, schema_xml)
    else
      Logger.info "SCHEMA\tCREATE[NOOP]\tWould have created schema: #{schema}"
    end
  end

  def riak_index_create(target, idx, schema, props) do
    if config :op do
      Logger.info "INDEX\tCREATE\tCreating Index: #{idx[:index]}"
      Riak.Search.Index.put(riak_pid(target), idx[:index], schema, props)
    else
      Logger.info "INDEX\tCREATE[NOOP]\tWould have created Index: #{idx[:index]}"
    end
  end

  def riak_get_schema( target, schema ) do
    Riak.Search.Schema.get(riak_pid(target), schema)
  end

end
