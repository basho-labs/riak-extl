defmodule RiakExtl do
  require Logger
  use Timex

  def main(args) do
    init_config

    config(:op, false)
    config(:json, false)
    config(:file, "riak-extl.json")

    configure_logger
    start_date = Date.now
    Logger.debug("Arguments recieved: #{args}")
    args |> parse_args |> set_config |> process
    end_date = Date.now
    Logger.info("Seconds taken: #{Date.diff(start_date,end_date, :secs)}")
  end

  def process({[],_}) do
    IO.puts "No commands given"
    print_help
  end

  def process([command|_args]) do

    config = load_file(config(:file))
    Logger.debug("Config loaded")
    Logger.debug("Source: pb://#{config["src_ip"]}:#{config["src_port"]}")
    Logger.debug("Target: pb://#{config["dest_ip"]}:#{config["dest_port"]}")

    case command do
      "ping" ->
        Logger.info("Connecting to source")
        start_riak(:src, String.to_atom(config["src_ip"]), config["src_port"])
        Logger.info("Pinging source...")
        Logger.info("Recieved [#{riak_ping(:src)}] from source")
        Logger.info("Connecting to target")
        start_riak(:dest, String.to_atom(config["dest_ip"]), config["dest_port"])
        Logger.info("Pinging target...")
        Logger.info("Recieved [#{riak_ping(:dest)}] from target")
      "create_indexes" ->
        case config(:type) do
          nil ->
            IO.puts "--type required for dry_run"
            print_help
          _ ->
            start_riak(:src, String.to_atom(config["src_ip"]), config["src_port"])
            start_riak(:dest, String.to_atom(config["dest_ip"]), config["dest_port"])
            Logger.debug("Command create_indexes starting")
            migrate_type_create_indexes
        end
      "sync" ->
        case config(:type) do
          nil ->
            IO.puts "--type required for sync"
            print_help
          _ ->
            start_riak(:src, String.to_atom(config["src_ip"]), config["src_port"])
            start_riak(:dest, String.to_atom(config["dest_ip"]), config["dest_port"])
            Logger.debug("Command SYNC starting")
            migrate_type
        end
      "list_src_buckets" ->
        case config(:type) do
          nil ->
            IO.puts "--type required for list_src_buckets"
            print_help()
          _ ->
            print_buckets(:src,
              String.to_atom(config["src_ip"]),
              config["src_port"])
        end
      "list_dest_buckets" ->
        case config(:type) do
          nil ->
            IO.puts "--type required for list_dest_buckets"
            print_help()
          _ ->
            print_buckets(:dest,
              String.to_atom(config["dest_ip"]),
              config["dest_port"])
        end
      "help" ->
        print_help()
      unknown ->
        Logger.warn "Unimplemented command: #{unknown}"
        print_help()
    end
  end

  defp load_file(file) do
    case File.regular?(file) do
      true -> JSON.decode!(File.read!(file))
      false -> %{}
    end
  end

  defp parse_args(args) do
    OptionParser.parse( args,
      strict: [type: :string, op: :boolean, json: :boolean],
      switches: [config: :string]
    )
  end

  defp set_config({_options, [], _errors}) do
    ["help"]
  end
  defp set_config({options, command, []}) do
    Enum.each([:op, :json, :type], fn attr ->
      if Dict.has_key?(options, attr) do
        config(attr, options[attr])
      end
    end)
    command
  end
  defp set_config({_options,_command,_errors}) do
    ["help"]
  end

  defp print_help() do
    IO.puts "Usage: ./riak-extl --type <bucket-type> [--config <config.json>] [--no-op|--op] [--no-json|--json] <command>"
    IO.puts "\t <bucket-type>\t\tThe bucket type to sync"
    IO.puts "\t <config.json>\t\tAn alternate config.json file (defaults to riak-extl.json)."
    IO.puts "\t [--no-op|--op]\t\tDisable or enable execution of changing destination clsuter"
    IO.puts "\t [--no-json|--json]\t\tDisable or enable JSON validation. JSON validation will error instead of writing invalid JSON values."
    IO.puts "\t <command>\t\tThe commant to execute. Could be one of:"
    IO.puts "\t\tping\t\tTest connectivity"
    IO.puts "\t\tlist_src_buckets\tList all buckets in <bucket-type> of source cluster."
    IO.puts "\t\tlist_dest_buckets\tList all buckets in <bucket-type> of destination cluster."
    IO.puts "\t\tsync\t\tPerform actual syncronization of buckets types from Source cluster to Destination/Target cluster"
    IO.puts "\t\tcreate_indexes\t\tMigrate Schemas, Indexes and Bucket configurations within <bucket-type>."
  end

####################
# Migrate Type Functions

  defp migrate_type do
    Logger.debug "Retrieving bucket list from source"
    src_buckets = get_buckets!(:src, config :type)
    Logger.debug "Retrieving bucket list from destination"
    dest_buckets = get_buckets!(:dest, config :type)
    type = config :type
    Enum.each(src_buckets, fn(b) ->
      migrate_bucket(type, b, Enum.member?(dest_buckets,b))
    end)
  end

  defp migrate_bucket(type, bucket, check_keys) do
    bucket_start = Date.now
    Logger.info "Migrating #{type}/#{bucket}"
    src_keys = get_keys!(:src, type, bucket)
    Logger.info "\t#{Enum.count(src_keys)} keys to sync"
    dest_keys = case check_keys do
      true -> get_keys!(:dest, type, bucket)
      false -> []
    end
    Logger.info "\t#{Enum.count(dest_keys)} keys in target"
    Enum.each(src_keys, fn(k) ->
      has_key = Enum.member?(dest_keys, k)
      migrate_key(type, bucket, k, has_key)
    end)
    bucket_end = Date.now
    Logger.info("Bucket sync in: #{Date.diff(bucket_start,bucket_end, :secs)} secs")
  end

  defp migrate_key(type, bucket, key, has_key) do
    src_o = get_obj!(:src, type, bucket, key)
    try do
      { action, obj } = case is_map(src_o) do
        true ->
          case has_key do
            true ->
              dest_o = get_obj!(:dest, type, bucket, key)
              case needs_update(src_o, dest_o) do
                true ->
                  {:put, %{src_o | vclock: dest_o.vclock}}
                false ->
                  {:skip, %{src_o | vclock: dest_o.vclock}}
              end
            false ->
              {:put, %{src_o | vclock: nil} }

          end
        false ->
          case has_key do
            true ->
              {:delete, src_o }
            false ->
              {:skip, src_o }
          end
      end

      res = case action do
        :skip -> Logger.debug "SKIP:\t #{key}"
        :put -> put_obj!(:dest, obj)
        :delete -> del_obj!(:dest, obj)
      end

    rescue
      e in ArgumentError ->
        Logger.error "Caught ArgumentError on #{type}/#{bucket}/#{key}"
        Logger.debug "ArgumentError: #{Exception.message(e)}"
      e in UndefinedFunctionError ->
        Logger.error "Caught UndefinedFunctionError on #{type}/#{bucket}/#{key}"
        Logger.debug "UndefinedFunctionError: #{Exception.message(e)}"
      e in FunctionClauseError ->
        Logger.error "Caught FunctionClauseError on #{type}/#{bucket}/#{key}"
        Logger.debug "FunctionClauseError: #{Exception.message(e)}"
        Process.exit(self(), "Fatal Error Occured")
    end
  end

  defp needs_update(src, dest) do
    cond do
      is_nil(src) and is_nil(dest) ->
        false
      is_nil(src) or is_nil(dest) ->
        true
      src.data !== dest.data ->
        true
      Riak.Object.get_all_metadata(src) !== Riak.Object.get_all_metadata(dest) ->
        true
      src.content_type !== dest.content_type ->
        true
      true ->
        false
    end
  end

######################
# Migrate Search functions

  defp migrate_type_create_indexes do
    Logger.debug "Retrieving bucket list from source"
    src_buckets = get_buckets!(:src, config :type)
    Logger.debug "Retrieving bucket list from destination"
    #dest_buckets = get_buckets!(:dest, options[:type])
    type = config :type
    queue = Enum.map(src_buckets, fn(b) ->
      migrate_bucket_create_indexes(type, b)
    end)

    Enum.each(queue, fn(q) -> bucket_configure(q) end)

  end

  defp bucket_configure(nil), do: nil
  defp bucket_configure({t,b,p}) do
    if config :op do
      case Riak.Bucket.put(riak_pid(:dest), {t,b}, p) do
        :ok -> Logger.info "BUCKET\tSUCCESS\tApplied configuration to #{b}"
        {:error, error} ->
          Logger.error "BUCKET\tERROR\tError applying props on #{b}"
          Logger.debug "BUCKET\tERROR\t#{b}: #{error}"
        _ -> Logger.error "BUCKET\tERROR\tUnknown error occured on #{b}"
      end
    else
      Logger.info "BUCKET\tNOOP\tWould have applied configuration to #{b}"
    end
  end

  defp migrate_bucket_create_indexes(type, bucket) do
    bucket_start = Date.now
    Logger.debug "Migrating Search for #{type}/#{bucket}"

    results = get_props!(type, bucket)
      |> get_idx_name
      |> get_index
      |> get_schema
      |> create_schema
      |> create_index
      |> queue_configure_bucket

    return = case results do
      { :add, props } -> Logger.debug "Recieved props to queue for bucket config"
        { type, bucket, props }
      :skip ->
        Logger.debug "Recieved SKIP configuration"
        nil
      nil ->
        Logger.debug "Recieved NIL migrating index, potential error"
        nil
      other ->
        Logger.debug "Recieved unknown result from migrating index, potential error"
        IO.inspect other
        nil
    end

    bucket_end = Date.now
    Logger.debug("Bucket Search sync in: #{Date.diff(bucket_start,bucket_end, :secs)} secs")

    return

  end

  defp get_props!(t,b) do
    { :ok, props } = Riak.Bucket.get(riak_pid(:src), {t,b})
    { t, b, props }
  end

  defp get_idx_name({t, b, props}), do: {t, b, props[:search_index]}

  defp get_index({_t, _b, nil}),  do: nil
  defp get_index({t, b, idx_name}) do
    { :ok, idx } = Riak.Search.Index.get(riak_pid(:src), idx_name)
    {t, b, idx, idx[:schema] }
  end

  defp get_schema(nil), do: nil
  defp get_schema({_t, _b, _idx, nil}), do: nil
  defp get_schema({t, b, idx, schema}) do
    { :ok, [name: schema, content: schema_xml] }
      = Riak.Search.Schema.get(riak_pid(:src), schema)
    {t, b, idx, schema, schema_xml}
  end

  defp create_schema(nil), do: nil
  defp create_schema({_t, _b, _idx, _schema, nil}), do: nil
  defp create_schema({t, b, idx, schema, schema_xml}) do
    case Riak.Search.Schema.get(riak_pid(:dest), schema) do
      { :ok, dest_schema } ->
        if ( cond do
          dest_schema[:name] !== schema -> false
          dest_schema[:schema_xml] !== schema_xml -> false
          true -> true
        end ) do
          Logger.warn "SCHEMA\tERROR\tDifferent schema exists for #{schema}"
          Logger.warn "SCHEMA\tERROR\tSkipping Index Creation due to schema error"
          nil
        else
          Logger.info "SCHEMA\tSKIP\tSchema already created and matches"
          {t, b, idx, schema}
        end
      { :error, "notfound" } ->
        put_schema(riak_pid(:dest), schema, schema_xml)
        {t, b, idx, schema}
      Error ->
        IO.puts "Unhandled response"
        IO.inspect Error
        Process.exit(self(),"Unhandled response from destination cluster getting schema")
        nil
    end
  end

  defp create_index(nil), do: nil
  defp create_index({t, b, idx, schema}) do
    { :ok, props } = Riak.Bucket.get(riak_pid(:dest), {t,b})
    case Riak.Search.Index.get(riak_pid(:dest), idx[:index]) do
      {:ok, dest_idx} ->
        cond do
          dest_idx[:schema] === schema ->
            Logger.info "INDEX\tSKIP\tIndex already created with right schema"
            {t, b, idx, props}
          dest_idx[:schema] !== schema ->
            Logger.info "INDEX\tERROR\tIndex already created with WRONG schema"
            nil
          true ->
            Logger.info "INDEX\tERROR\tUnable to compare indexes"
            nil
        end
      {:error, "notfound"} ->
        riak_index_create(riak_pid(:dest), idx, schema, [n_val: props[:n_val]])
        {t, b, idx, props}
      Error ->
        IO.puts "Unhandled response"
        IO.inspect Error
        Process.exit(self(),"Unhandled response from destination cluster getting index")
        nil
    end
  end

  defp riak_index_create(pid, idx, schema, props) do
    if config :op do
      Logger.info "INDEX\tCREATE\tCreating Index: #{idx[:index]}"
      :riakc_pb_socket.create_search_index(pid, idx[:index], schema, props)
    else
      Logger.info "INDEX\tCREATE[NOOP]\tWould have created Index: #{idx[:index]}"
    end
  end

  defp queue_configure_bucket(nil), do: nil
  defp queue_configure_bucket({_t, _b, idx, props}) do
    cond do
      !Keyword.has_key?(props, :search_index) ->
        Logger.info "BUCKET\tQUEUE\tQueueing bucket configuration"
        dest_props = [search_index: idx[:index]]
        {:add, dest_props}
      props[:search_index] === idx[:index] ->
        Logger.info "BUCKET\tSKIP\tBucket already configured for #{idx[:index]}"
        :skip
      true ->
        Logger.error "BUCKET\tERROR\tBucket has non-matching search index configured"
        nil
    end
  end

  defp print_buckets(target, ip, port) when is_atom(target) do
    start_riak :src, ip, port
    buckets = Riak.Bucket.Type.list!(riak_pid(:src), config :type )
    Enum.each(buckets, fn(b) -> Logger.info "Bucket: #{b}" end)
  end

  defp init_config do
    {:ok, agent} = Agent.start_link fn -> %{} end
    Process.register(agent, :config)
  end

  defp config(attr, val) do
    Agent.update(Process.whereis(:config), fn map -> Map.put(map, attr, val) end)
  end

  defp config(attr) do
    Agent.get(Process.whereis(:config), fn map -> Map.get map, attr end)
  end

  defp configure_logger do
    timestamp = Date.now |> DateFormat.format!("%Y%m%d-%H%M%S", :strftime)
    Logger.add_backend {LoggerFileBackend, :file}
  	Logger.configure_backend {LoggerFileBackend, :file},
      path: "./riak-extl-#{timestamp}.log",
      level: :debug
    Logger.configure_backend :console, [ level: :info ]
  end

  defp put_schema(pid, schema, schema_xml) do
    if config :op do
      Logger.info "SCHEMA\tCREATE\tCreating Schema: #{schema}"
      Riak.Search.Schema.create(pid, schema, schema_xml)
    else
      Logger.info "SCHEMA\tCREATE[NOOP]\tWould have created schema: #{schema}"
    end
  end

  defp get_obj!( target, type, bucket, key ) when is_atom(target) do
    Riak.find(riak_pid(target), { type, bucket }, key)
  end

  defp put_obj!( target, obj ) do
    put_obj!(target, obj, config(:op), config(:json))
  end

  defp put_obj!( target, obj, op, true ) when is_atom(target) do
    case JSON.decode(obj.data) do
      {:ok, _ } -> :ok
        put_obj!( target, obj, op, false )
      {:error, :unexpected_end_of_buffer } ->
        Logger.warn "PUT ERROR\tInvalid JSON: Unexpected End of Buffer for key #{obj.key}"
      {:error, {:unexpected_token, token }} ->
        Logger.warn "PUT ERROR\tInvalid JSON: Unexpected Token: #{token} for key #{obj.key}"
      _ ->
        Logger.warn "PUT ERROR\tInvalid JSON: Unknown Error for key #{obj.key}"
    end
  end

  defp put_obj!( target, obj, false, false ) when is_atom(target) do
    Logger.debug "NOOP [PUT]:\t #{obj.key}"
  end

  defp put_obj!( target, obj, true, false ) when is_atom(target) do
    Logger.debug "PUT:\t #{obj.key}"
    Riak.put(riak_pid(target), obj)
  end

  defp del_obj!( target, obj ) when is_atom(target) do
    case config :op do
      false ->
        Logger.debug "NOOP [DEL]:\t #{obj.key}"
      true ->
        Logger.debug "DEL:\t #{obj.key}"
        Riak.delete(riak_pid(target), obj)
    end
  end

  defp get_keys!( target, type, bucket ) when is_atom(target) do
    Logger.debug "Retrieving list of keys from #{type}/#{bucket} on #{to_string(target)}"
    Riak.Bucket.keys!(riak_pid(target), {type, bucket})
  end

  defp get_buckets!( target, type ) when is_atom (target) do
    Riak.Bucket.Type.list!(riak_pid(target), type)
  end

  defp riak_ping( target ) when is_atom (target) do
    Riak.ping(riak_pid(target))
  end

  defp start_riak( target, ip, port ) when is_atom(target) do
    Logger.debug "Connecting to Riak [#{to_string(target)}]"
    { :ok, riak } = Riak.Connection.start(ip, port)
    Process.register(riak, target)
    Logger.debug "Connection successful"
    :ok
  end

  defp riak_pid(target) when is_atom (target) do
    pid = Process.whereis(target)
    cond do
      is_nil pid ->
        Process.exit self(), "Riak Connection to #{to_string target} Broken"
        :nil
      !Process.alive? pid ->
        Process.exit self(), "Riak Connection to #{to_string target} Broken"
        :nil
      true ->
        pid
    end
  end
end
