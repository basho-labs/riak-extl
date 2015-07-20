Riak-Extl
========

Synchronization script for client

Client is using strong consistency, so MDC was unavailable.

```
Usage: ./riak-extl --type <bucket-type> [--config <config.json>] [--no-op|--op] [--no-json|--json] <command>
         <bucket-type>          The bucket type to sync
         <config.json>          An alternate config.json file (defaults to riakextl.json).
         [--no-op|--op]         Disable or enable execution of changing destination clsuter
         [--no-json|--json]             Disable or enable JSON validation. JSON validation will error instead of writing invalid JSON values.
         <command>              The command to execute. Could be one of:
                ping            Test connectivity
                list_src_buckets        List all buckets in <bucket-type> of source cluster.
                list_sink_buckets       List all buckets in <bucket-type> of sink cluster.
                sync            Perform actual synchronization of buckets types from Source cluster to sink cluster
                create_indexes          Migrate Schemas, Indexes and Bucket configurations within <bucket-type>.
```

### Instructions

#### Requirements

Requirements for building:

  * Erlang 17+
  * Elixir 1.0.4
  * git

Requirements for deployment:

  * Erlang 17+

#### Building

  git clone https://github.com/basho-labs/riak-extl
  mix deps.get
  mix
  mix escript.build

#### Configuring

  Modify riak-extl.json [ or specified file via --config ] for connection parameters

#### Running

  ./riak-extl
