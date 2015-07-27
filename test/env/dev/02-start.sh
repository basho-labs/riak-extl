#!/bin/bash
for n in {3..4}; do

dev$n/bin/riak start
dev$n/bin/riak-admin bucket-type create consistent '{"props":{"consistent":false,"n_val":1}}'
dev$n/bin/riak-admin bucket-type create search '{"props":{"consistent":false,"n_val":1}}'
dev$n/bin/riak-admin bucket-type activate consistent
dev$n/bin/riak-admin bucket-type activate search

done
