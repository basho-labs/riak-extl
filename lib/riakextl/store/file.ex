defmodule RiakExtl.Store.File do
  use GenServer
  require Logger
  import RiakExtl.Util

  def init({dir}) do
    state = HashDict.new()
    state = HashDict.put(state, {:state, :base}, dir)
    {:ok, state}
  end

  def terminate(reason, _state) do
    Logger.warn("#{__MODULE__} closing with reason: #{to_str(reason)}")
    close_dets
    :ok
  end

  def run(pid, mod, fun, args) do
    GenServer.call(pid, {mod, fun, args})
  end

  def handle_call({Riak, :ping, _args}, _from, state) do
    {:reply, :pong, state}
  end

  def handle_call({Riak, :put, [obj]}, _from, state) do
    {:ok, state} = bucket_put(obj.bucket, {:key, obj.key}, obj, state)
    {:reply, :ok, state}
  end

  def handle_call({Riak, :find, [{type, bucket}, key]}, _from, state) do
    {reply, state} = bucket_get({type, bucket}, {:key, key}, state)
    {:reply, reply, state}
  end

  def handle_call({Riak, :delete, [obj]}, _from, state) do
    reply = case bucket_exists?({obj.type, obj.bucket}, state) do
      true ->
        {ref, state} = get_bucket_ref({obj.type, obj.bucket}, state)
        :dets.delete(ref, obj.key)
      _ ->
        Logger.info "Key requested but bucket doesn't exist"
        :ok
    end
    {:reply, reply, state}
  end

  def handle_call({Riak.Bucket, :keys!, [{type, bucket}]}, _from, state) do
    reply = case bucket_exists?({type, bucket}, state) do
      true ->
        {ref, state} = get_bucket_ref({type, bucket}, state)
        :dets.match(ref, {{:key, :"$1"}, :_}) |> List.flatten
      _ ->
        Logger.warn("Bucket list requested on non existant bucket")
        {:error, :nobucket}
    end
    {:reply, reply, state}
  end

  def handle_call({Riak.Bucket.Type, :list!, [type]}, _from, state) do
    base = HashDict.fetch!(state, {:state, :base})
    {reply, state} = case File.ls("#{base}/#{type}") do
      {:ok, files} ->
        bfiles = Enum.filter(files, fn n ->
          Regex.match?(~r/\.bucket$/, n)
        end)
        load_buckets(bfiles, type, state)
      {:error, :enoent} ->
        {[], state}
      _unknown ->
        Logger.warn("Listing files in #{base}/#{type} returned unknown error")
        {[], state}
    end
    {:reply, reply, state}
  end

  def handle_call({Riak.Bucket, :put, [{type, bucket}, props]}, _from, state) do
    {:ok, state} = bucket_put({type, bucket}, {:meta, :props}, props, state)
    {:reply, :ok, state}
  end

  def handle_call({Riak.Bucket, :get, [{type, bucket}]}, _from, state) do
    {reply, state} = case bucket_get({type, bucket}, {:meta, :props}, state) do
      {{:error, :notfound}, state} ->
        Logger.warn "Bucket Props not found for #{type}/#{bucket}"
        {{:ok, [n_val: 5]}, state}
      {{:error, e}, state} -> {{:error, e}, state}
      {return, state} -> {{:ok, return}, state}
    end
    {:reply, reply, state}
  end

  def handle_call({Riak.Search.Schema, :create, [schema, schema_xml]}, _from, state) do
    schema_obj = [name: schema, content: schema_xml]
    {:ok, state} = create_search(:schema, schema, schema_obj, state)
    {:reply, :ok, state}
  end

  def handle_call({Riak.Search.Schema, :get, [schema]}, _from, state) do
    {reply, state} = get_search(:schema, schema, state)
    {:reply, reply, state}
  end

  def handle_call({Riak.Search.Index, :put, [index, schema, props]}, _from, state) do
    {:ok, state} = create_search(:index, index, props ++ [schema: schema, index: index], state)
    {:reply, :ok, state}
  end

  def handle_call({Riak.Search.Index, :get, [index]}, _from, state) do
    {reply, state} = get_search(:index, index, state)
    {:reply, reply, state}
  end

  def handle_call({Riak.Connection, :stop, []}, _from, state) do
    #terminate("Riak.Connection.stop/1", state)
    close_dets
    {:reply, :ok, state}
  end

  def handle_call({unknown, _args}, _from, state) do
    Logger.warn("Unimplemented File API called: #{unknown}")
    {:reply, {:error, "Unhandled API Call"}, state}
  end

  def bucket_put({type, bucket}, key, value, state) do
    {ref, state} = get_bucket_ref({type, bucket}, state)
    :ok = :dets.insert(ref, {key, :erlang.term_to_binary(value)})
    {:ok, state}
  end

  def bucket_get({type, bucket}, key, state) do
    {ref, state} = get_bucket_ref({type, bucket}, state)
    reply = case :dets.lookup(ref, key) do
      [] -> {:error, :notfound}
      [{^key, obj}] -> :erlang.binary_to_term(obj)
      [{^key, obj}|_tail] ->
        Logger.warn "Sibling found in bucket"
        :erlang.binary_to_term(obj)
      error ->
        Logger.warn "Unknown response for key"
        {:error, error}
    end
    {reply, state}
  end

  def get_bucket_ref({type, bucket}, state) do
    case bucket_exists?({type, bucket}, state) do
      true ->
        ref = HashDict.fetch!(state,{:bucket, {type, bucket}})
        {ref, state}
      _ ->
        ref = create_bucket_ref({type, bucket}, state)
        state = HashDict.put(state,{:bucket, {type, bucket}},ref)
        {ref, state}
    end
  end

  def bucket_exists?({type, bucket}, state), do:
    HashDict.has_key?(state,{:bucket, {type, bucket}})

  def load_buckets(files, type, state) do
    load_buckets(files, [], type, state)
  end

  def load_buckets([], buckets, _type, state) do
    {buckets, state}
  end

  def load_buckets([filename|files], buckets, type, state) do
    {{_type, bucket}, state} = bucket_ref_from_file(filename, type, state)
    load_buckets(files, buckets ++ [bucket], type, state)
  end

  def bucket_ref_from_file(filename, type, state) do
    base = HashDict.fetch!(state, {:state, :base})
    case :dets.open_file("#{base}/#{type}/#{filename}",[repair: false]) do
      {:ok, ref} ->
      meta = case :dets.lookup(ref,{:meta, :name}) do
        [{{:meta, :name}, meta}] -> meta
        [{{:meta, :name}, meta}|_tail] ->
          Logger.warn("Multiple meta entries for #{filename} found")
          meta
        _unknown ->
          raise "Unknown bucket #{filename}, no metadata found"
      end
      case :erlang.binary_to_term(meta) do
        {type, bucket} ->
          state = HashDict.put(state,{:bucket, {type, bucket}},ref)
          {{type, bucket}, state}
        _unknown ->
          raise "Unable to load #{filename}"
      end
      {:error, {:needs_repair, filename}} ->
        Logger.warn("file #{filename} not closed properly and needs repair")
      unknown ->
        IO.inspect(unknown)
        raise "Unable to open #{filename}"
    end
  end

  def create_bucket_ref({type, bucket}, state) do
    base = HashDict.fetch!(state, {:state, :base})
    dir = "#{base}/#{type}"
    :ok = File.mkdir_p!(dir)
    filename = :crypto.hash(:sha, bucket) |> Base.url_encode64
    {:ok, ref} = :dets.open_file("#{dir}/#{filename}.bucket",[repair: false])

    case :dets.insert_new(
      ref,
      {{:meta,:name},
      :erlang.term_to_binary({type, bucket})}
    ) do
      true ->
        Logger.debug("Created new file #{dir}/#{filename}.bucket")
      false ->
        Logger.debug("Opened #{dir}/#{filename}.bucket")
      end
    ref
  end

  def init_search(state) do
    base = HashDict.fetch!(state, {:state, :base})
    dir = "#{base}"
    :ok = File.mkdir_p!(dir)
    {:ok, ref} = :dets.open_file("#{dir}/search.indexes",[])
    state = HashDict.put(state, {:meta, :search}, ref)
    {:ok, state}
  end

  def get_search_ref(state) do
    state = case HashDict.has_key?(state,{:meta, :search}) do
      true -> state
      _ ->
        {:ok, state} = init_search(state)
        state
    end
    ref = HashDict.fetch!(state,{:meta, :search})
    {ref, state}
  end

  def create_search(type, id, content, state) do
    {ref, state} = get_search_ref(state)
    :ok = :dets.insert(ref, {{type, id}, :erlang.term_to_binary(content)})
    {:ok, state}
  end

  def get_search(type, id, state) do
    {ref, state} = get_search_ref(state)
    result = case :dets.lookup(ref, {type, id}) do
      [{{^type, ^id}, result}] -> {:ok, :erlang.binary_to_term(result)}
      [{{^type, ^id}, result}|_extra] ->
        Logger.info("Multiple results for #{type}:#{id} returned")
        {:ok, :erlang.binary_to_term(result)}
      [] -> {:error, "notfound"}
      _ ->
        Logger.info("unknown result for #{type}:#{id} returned")
    end
    {result, state}
  end

  def close_dets do
    for dets <- :dets.all do
      Logger.debug("Closing #{dets}")
      :dets.close(dets)
    end
  end

end
