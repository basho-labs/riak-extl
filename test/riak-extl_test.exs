defmodule RiakExtlTest do
  use ExUnit.Case, async: false
  import Mock

  test_with_mock "start/register/ping riak", Riak.Connection, [start: fn( _ip, _port ) -> { :ok, self() } end] do
    assert :ok = RiakExtl.start_riak(:start_test, "127.0.0.1", 8087), "Riak Started"
    assert is_pid(Process.whereis(:start_test))
    assert is_pid(RiakExtl.riak_pid(:start_test))
    assert_raise RuntimeError, fn ->
      RiakExtl.riak_pid(:unknown)
    end
    assert_raise RuntimeError, fn ->
      pid = spawn fn -> Process.register(self(), :test) end
      with_mock Process, [whereis: fn(_name) -> pid end, alive?: fn(_pid) -> false end ] do
        refute pid == RiakExtl.riak_pid(:test)
      end
    end
    with_mock Riak, [ping: fn(_pid) -> :pong end] do
      assert :pong = RiakExtl.riak_ping(:start_test)
    end
  end


  test_with_mock "Data Loads", Riak.Bucket.Type, [list!: fn(_pid, type) -> ["#{type}_bucket_1", "#{type}_bucket_2", "#{type}_bucket_3"] end] do
    with_mock Riak.Bucket, [
      keys!: fn(_pid, {_type, _bucket}) -> ["fookey1", "fookey2", "fookey3"] end,
      put: fn(_pid, {_t,_b}, _p) -> :ok end,
      get: fn(_pid, {_t,_b}) -> {:ok, [n_val: 3]} end
      ] do
        with_mock Riak.Search.Index, [
          get: fn(_pid, idx_name) -> {:ok, [index: idx_name, schema: idx_name]} end,
          put: fn(_pid, _idx, _schema, _props) -> :ok end
        ] do
        with_mock Riak.Search.Schema, [
          create: fn(_pid, _schema, _schema_xml) -> :ok end,
          get: fn(_pid, schema) -> {:ok, [name: schema, content: "<xml></xml>"]} end
          ] do
          with_mock Riak, [
            delete: fn(_pid, _obj) -> :ok end,
            find: fn(_pid, {type, bucket}, key) ->
              Riak.Object.create(bucket: bucket, type: type, key: key, data: "{}")
            end,
            put: fn(_pid, _obj) -> :ok end
          ] do
            with_mock Logger, [:passthrough], [] do
              #assert :ok = RiakExtl.configure_logger

              assert true = RiakExtl.init_config
              assert :ok = RiakExtl.config(:test, "value")
              assert "value" = RiakExtl.config :test

              RiakExtl.config(:type, "foo")

              {:ok, src } = Agent.start_link fn -> [] end
              {:ok, dest } = Agent.start_link fn -> [] end
              Process.register(src, :src)
              Process.register(dest, :dest)

              buckets = RiakExtl.get_buckets!(:src, "foo")
              assert buckets = ["foo_bucket_1", "foo_bucket_2", "foo_bucket_3"]

              assert :ok = RiakExtl.print_buckets(:src)

              keys = RiakExtl.get_keys!(:src,"foo","foo_bucket_1")
              assert keys = ["fookey1", "fookey2", "fookey3"]

              obj = RiakExtl.get_obj!(:src,"foo","foo_bucket_1","fookey1")
              assert obj == Riak.Object.create(bucket: "foo_bucket_1", type: "foo", key: "fookey1", data: "{}")


              RiakExtl.config(:op, false)
              RiakExtl.config(:json, false)
              assert :ok = RiakExtl.put_obj!(:src, obj)
              assert :ok = RiakExtl.del_obj!(:src, obj)

              RiakExtl.config(:op, true)
              assert :ok = RiakExtl.put_obj!(:src, obj)
              assert :ok = RiakExtl.del_obj!(:src, obj)

              RiakExtl.config(:json, true)
              assert :ok = RiakExtl.put_obj!(:src, obj)
              # should fail to put, should test to be sure of this
              obj = %{obj | data: ""}
              assert :ok = RiakExtl.put_obj!(:src, obj)
              # should fail to put, should test to be sure of this
              obj = %{obj | data: "]"}
              assert :ok = RiakExtl.put_obj!(:src, obj)

              assert {:src, "foo", [], "foo_schema_1", "<xml></xml>"} = RiakExtl.get_schema({:src,"foo",[],"foo_schema_1"})

              assert :ok = RiakExtl.put_schema(:src, "foo_schema_1", "<xml></xml>")

              assert :ok = RiakExtl.migrate_type
              assert :ok = RiakExtl.migrate_type_create_indexes


            end
          end
        end
      end
    end
  end
end
