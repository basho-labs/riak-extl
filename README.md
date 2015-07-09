RiakExtl
========

Synchronization script for client

Client is using strong consistency, so MDC was unavailable.

```
Usage: ./riak-extl --type <bucket-type> [--config <config.json>] <command>
         <bucket-type>          The bucket type to sync
         <config.json>          An alternate config.json file (defaults to riakextl.json).
         <command>              The command to execute. Could be one of:
                ping            Test connectivity
                list_src_buckets        List all buckets in <bucket-type> of source cluster.
                list_dest_buckets       List all buckets in <bucket-type> of destination cluster.
                dry_run         Perform a no-op sync, performing comparisons, but skipping creates/updates.
                sync            Perform actual synchronization of buckets types from Source cluster to Destination/Target cluster
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

  git clone https://github.com/basho-labs/riakextl
  mix deps.get
  mix
  mix escript.build

#### Configuring

  Modify riakextl.json [ or specified file via --config ] for connection parameters

#### Running

  ./riak-extl
