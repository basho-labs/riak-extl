Riak-Extl
========

Synchronization script

```
Usage: ./riak-extl --type <bucket-type> [--op] [--json] <command>
  <command> (See COMMANDS section below)
         ping | sync | sync_to_fs | sync_from_fs
         sync_indexes | sync_indexes_to_fs | sync_indexes_from_fs
  --type <bucket-type>  The bucket type to sync
  [--no-op|--op]        Disable or enable modifications to sink cluster
  [--no-json|--json]    Disable or enable JSON validation.
        JSON validation will error instead of writing invalid JSON values.
  COMMANDS:
        ping                    Test connectivity
        sync                    Synchronize <bucket-type>: SOURCE -> SINK
        sync_to_fs              Synchronize <bucket-type>: SOURCE -> FS
        sync_from_fs            Synchronize <bucket-type>: FS -> SINK
        sync_indexes            Synchronize Schema/Index/Bucket: SOURCE -> SINK
        sync_indexes_to_fs      Synchronize Schema/Index/Bucket: SOURCE -> FS
        sync_indexes_from_fs    Synchronize Schema/Index/Bucket: FS -> SINK
        showcfg                 Display loaded configuration options
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

  ```
  git clone https://github.com/basho-labs/riak-extl
  mix deps.get
  mix
  mix escript.build
  ```

#### Configuring

  Modify ```config/<Mix.env>.exs``` with configuration parameters

#### Running

  ./riak-extl

#### Installing Dependencies

##### Ubuntu

    ```
    wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
    sudo dpkg -i erlang-solutions_1.0_all.deb
    sudo apt-get update
    sudo apt-get -y install esl-erlang=1:17.5 elixir=1.0.4-1 git
    ```

###### ***NOTE:***
If your compile dies or errors while compiling ```tzdata```, it's likely your system needs more memory.
