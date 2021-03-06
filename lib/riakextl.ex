defmodule RiakExtl do
  require Logger
  use Timex
  import RiakExtl.Store
  import RiakExtl.Config
  import RiakExtl.Util


  def main(args) do
    init_config

    config(:op, false)
    config(:json, false)
    config(:config, "riak-extl.conf")

    configure_logger
    start_date = Date.now
    Logger.debug("Arguments recieved: #{args}")
    args |> parse_args |> set_config |> process
    end_date = Date.now
    Logger.info("Seconds taken: #{Date.diff(start_date,end_date, :secs)}")
  end

  defp process(command) do
    Logger.debug("Source: pb://#{config :src_ip}:#{config :src_port}")
    Logger.debug("Target: pb://#{config :sink_ip}:#{config :sink_port}")

    case command do
      "ping" ->
        load_config
        Logger.info("Connecting to source")
        start_riak(:src, config(:src_ip), config(:src_port))
        IO.puts("Where is :src:")
        IO.inspect(Process.whereis(:src))
        Logger.info("Pinging source...")
        Logger.info("Recieved [#{riak_ping(:src)}] from source")
        Logger.info("Connecting to target")
        start_riak(:sink, config(:sink_ip), config(:sink_port))
        Logger.info("Pinging target...")
        Logger.info("Recieved [#{riak_ping(:sink)}] from target")
      "sync_indexes" ->
        load_config
        case config(:type) do
          nil ->
            IO.puts "--type required for #{command}"
            print_help
          _ ->
            start_riak(:src, config(:src_ip), config(:src_port))
            start_riak(:sink, config(:sink_ip), config(:sink_port))
            Logger.debug("Command #{command} starting")
            migrate_type_create_indexes
            stop(:src)
            stop(:sink)
        end
      "sync_indexes_to_fs" ->
        load_config
        case config(:type) do
          nil ->
            IO.puts "--type required for #{command}"
            print_help
          _ ->
            start_riak(:src, config(:src_ip), config(:src_port))
            start_file(:sink, config :sink_dir)
            Logger.debug("Command #{command} starting")
            migrate_type_create_indexes
            stop(:src)
            stop(:sink)
        end
      "sync_indexes_from_fs" ->
        load_config
        case config(:type) do
          nil ->
            IO.puts "--type required for #{command}"
            print_help
          _ ->
            start_file(:src, config :src_dir)
            start_riak(:sink, config(:sink_ip), config(:sink_port))
            Logger.debug("Command #{command} starting")
            migrate_type_create_indexes
            stop(:src)
            stop(:sink)
        end
      "sync" ->
        load_config
        case config(:type) do
          nil ->
            IO.puts "--type required for #{command}"
            print_help
          _ ->
            start_riak(:src, config(:src_ip), config(:src_port))
            start_riak(:sink, config(:sink_ip), config(:sink_port))
            Logger.debug("Command #{command} starting")
            migrate_type
            stop(:src)
            stop(:sink)
        end
      "sync_to_fs" ->
        load_config
        case config(:type) do
          nil ->
            IO.puts "--type required for #{command}"
            print_help
          _ ->
            start_riak(:src, config(:src_ip), config(:src_port))
            start_file(:sink, config :sink_dir)
            Logger.debug("Command #{command} starting")
            migrate_type
            stop(:src)
            stop(:sink)
        end
      "sync_from_fs" ->
        load_config
        case config(:type) do
          nil ->
            IO.puts "--type required for #{command}"
            print_help
          _ ->
            start_file(:src, config :src_dir)
            start_riak(:sink, config(:sink_ip), config(:sink_port))
            Logger.debug("Command #{command} starting")
            migrate_type
            stop(:src)
            stop(:sink)
        end
      "help" ->
        print_help()
      "showcfg" ->
        load_config
        IO.inspect(get_config)
      unknown ->
        Logger.warn "Unimplemented command: #{unknown}"
        print_help()
    end
  end

  defp parse_args(args) do
    OptionParser.parse( args,
      strict: [type: :string, op: :boolean, json: :boolean],
      switches: [config: :string, op: :boolean, json: :boolean]
    )
  end

  defp set_config({_options, [], _errors}) do
    ["help"]
  end
  defp set_config({options, [command], []}) do
    Enum.each([:op, :json, :type, :config], fn attr ->
      if Dict.has_key?(options, attr) do
        config(attr, options[attr])
      end
    end)
    command
  end
  defp set_config({_options,_command,_errors}) do
    Logger.warn "Invalid command line options"
    ["help"]
  end

  defp load_config do
    case File.read(config :config) do
    {:ok, data} ->
      String.split(data, "\n")
      |> Enum.filter(&String.contains?(&1, "="))
      |> Enum.each(
        fn line ->
          [key, value] = String.split(line, "=")
          key = String.strip(key) |> String.to_atom
          value = case key do
            :src_port -> value |> String.strip |> String.to_integer
            :sink_port -> value |> String.strip |> String.to_integer
            :src_ip -> value |> String.strip |> String.to_atom
            :sink_ip -> value |> String.strip |> String.to_atom
            _ -> value |> String.strip
          end
          config_new key, value
        end)
      {:error, error} ->
        Logger.warn("Error loading configuration file [#{config :config}]")
        Logger.warn("Please use riak-extl.conf-example as example to specify connection information")
        Logger.warn("Error: #{to_str(error)}")
        Process.exit(self(), "Config file error")
      end
  end

  defp print_help() do
    IO.puts "Usage: ./riak-extl --type <bucket-type> [--op] [--json] <command>"
    IO.puts "  <command> (See COMMANDS section below)"
    IO.puts "\t ping | sync | sync_to_fs | sync_from_fs"
    IO.puts "\t sync_indexes | sync_indexes_to_fs | sync_indexes_from_fs"
    IO.puts "  --type <bucket-type>\tThe bucket type to sync"
    IO.puts "  [--no-op|--op]\tDisable or enable modifications to sink cluster"
    IO.puts "  [--no-json|--json]\tDisable or enable JSON validation."
    IO.puts "\tJSON validation will error instead of writing invalid JSON values."
    IO.puts "  COMMANDS:"
    IO.puts "\tping\t\t\tTest connectivity"
    IO.puts "\tsync\t\t\tSynchronize <bucket-type>: SOURCE -> SINK"
    IO.puts "\tsync_to_fs\t\tSynchronize <bucket-type>: SOURCE -> FS"
    IO.puts "\tsync_from_fs\t\tSynchronize <bucket-type>: FS -> SINK"
    IO.puts "\tsync_indexes\t\tSynchronize Schema/Index/Bucket: SOURCE -> SINK"
    IO.puts "\tsync_indexes_to_fs\tSynchronize Schema/Index/Bucket: SOURCE -> FS"
    IO.puts "\tsync_indexes_from_fs\tSynchronize Schema/Index/Bucket: FS -> SINK"
    IO.puts "\tshowcfg\t\t\tDisplay loaded configuration options"
  end

####################
# Migrate Type Functions

  defp migrate_type do
    Logger.debug "Retrieving bucket list from source"
    src_buckets = get_buckets!(:src, config :type)
    Logger.debug "Retrieving bucket list from destination"
    sink_buckets = get_buckets!(:sink, config :type)
    type = config :type
    Enum.each(src_buckets, fn(b) ->
      migrate_bucket(type, b, Enum.member?(sink_buckets,b))
    end)
  end

  defp migrate_bucket(type, bucket, check_keys) do
    bucket_start = Date.now
    Logger.info "Migrating #{type}/#{bucket}"
    src_keys = get_keys!(:src, type, bucket)
    Logger.info "\t#{Enum.count(src_keys)} keys to sync"
    sink_keys = case check_keys do
      true -> get_keys!(:sink, type, bucket)
      false -> []
    end
    Logger.info "\t#{Enum.count(sink_keys)} keys in target"
    Enum.each(src_keys, fn(k) ->
      has_key = Enum.member?(sink_keys, k)
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
              case get_obj!(:sink, type, bucket, key) do
                sink_o when is_map(sink_o) ->
                  case needs_update(src_o, sink_o) do
                    true ->
                      {:put, %{src_o | vclock: sink_o.vclock}}
                    false ->
                      {:skip, %{src_o | vclock: sink_o.vclock}}
                  end
                {:error, :notfound} ->
                  Logger.info("#{key} listed, but not found in #{bucket}")
                  {:put, %{src_o | vclock: nil} }
                {:error, _ } ->
                  Logger.warn("unknown error returned getting #{key} in #{bucket}")
                _ ->
                  Logger.warn("unknown result getting #{key} from #{bucket} in sink")
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
        :put -> put_obj!(:sink, obj)
        :delete -> del_obj!(:sink, obj)
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

  defp needs_update(src, sink) do
    cond do
      is_nil(src) and is_nil(sink) ->
        false
      is_nil(src) or is_nil(sink) ->
        true
      src.data !== sink.data ->
        true
      Riak.Object.get_all_metadata(src) !== Riak.Object.get_all_metadata(sink) ->
        true
      src.content_type !== sink.content_type ->
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
    type = config :type
    queue = Enum.map(src_buckets, fn(b) ->
      migrate_bucket_create_indexes(type, b)
    end)

    Enum.each(queue, fn(q) -> bucket_configure(q) end)

  end

  defp bucket_configure(nil), do: nil
  defp bucket_configure({t,b,p}) do
    if config :op do
      case put_bucket(:sink, {t,b}, p) do
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
    { :ok, props } = get_bucket(:src, {t,b})
    { t, b, props }
  end

  defp get_idx_name({t, b, props}), do: {t, b, props[:search_index]}

  defp get_index({_t, _b, nil}),  do: nil
  defp get_index({t, b, idx_name}) do
    { :ok, idx } = riak_get_index(:src, idx_name)
    {t, b, idx, idx[:schema] }
  end

  defp get_schema(nil), do: nil
  defp get_schema({_t, _b, _idx, nil}), do: nil
  defp get_schema({t, b, idx, schema}) do
    { :ok, [name: schema, content: schema_xml] }
      = riak_get_schema(:src, schema)
    {t, b, idx, schema, schema_xml}
  end

  defp create_schema(nil), do: nil
  defp create_schema({_t, _b, _idx, _schema, nil}), do: nil
  defp create_schema({t, b, idx, schema, schema_xml}) do
    case riak_get_schema(:sink, schema) do
      { :ok, sink_schema } ->
        if Dict.equal?([name: schema, content: schema_xml], sink_schema) do
          Logger.info "SCHEMA\tSKIP\tSchema already created and matches"
          {t, b, idx, schema}
        else
          Logger.warn "SCHEMA\tERROR\tDifferent schema exists for #{schema}"
          Logger.warn "SCHEMA\tERROR\tSkipping Index Creation due to schema error"
          nil
        end
      { :error, "notfound" } ->
        put_schema(:sink, schema, schema_xml)
        {t, b, idx, schema}
      error ->
        IO.puts "Unhandled response"
        IO.inspect error
        Process.exit(self(),"Unhandled response from destination cluster getting schema")
        nil
    end
  end

  defp create_index(nil), do: nil
  defp create_index({t, b, idx, schema}) do
    { :ok, props } = get_bucket(:sink, {t,b})
    case riak_get_index(:sink, idx[:index]) do
      {:ok, sink_idx} ->
        cond do
          sink_idx[:schema] === schema ->
            Logger.info "INDEX\tSKIP\tIndex already created with right schema"
            {t, b, idx, props}
          sink_idx[:schema] !== schema ->
            Logger.info "INDEX\tERROR\tIndex already created with WRONG schema"
            nil
          true ->
            Logger.info "INDEX\tERROR\tUnable to compare indexes"
            nil
        end
      {:error, "notfound"} ->
        riak_index_create(:sink, idx, schema, [n_val: props[:n_val]])
        {t, b, idx, props}
      error ->
        IO.puts "Unhandled response"
        IO.inspect error
        Process.exit(self(),"Unhandled response from destination cluster getting index")
        nil
    end
  end

  defp queue_configure_bucket(nil), do: nil
  defp queue_configure_bucket({_t, _b, idx, props}) do
    cond do
      !Keyword.has_key?(props, :search_index) ->
        Logger.info "BUCKET\tQUEUE\tQueueing bucket configuration"
        sink_props = [search_index: idx[:index]]
        {:add, sink_props}
      props[:search_index] === idx[:index] ->
        Logger.info "BUCKET\tSKIP\tBucket already configured for #{idx[:index]}"
        :skip
      true ->
        Logger.error "BUCKET\tERROR\tBucket has non-matching search index configured"
        nil
    end
  end

  defp configure_logger do
    timestamp = Date.now |> DateFormat.format!("%Y%m%d-%H%M%S", :strftime)
    Logger.add_backend {LoggerFileBackend, :file}
  	Logger.configure_backend {LoggerFileBackend, :file},
      path: "./riak-extl-#{timestamp}.log",
      level: :debug
    Logger.configure_backend :console, [ level: :info ]
  end

end
