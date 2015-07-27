#!/bin/bash
base="localhost:10038"
b=10
k=10
s=3

curl $base/search/schema/_yz_default -o schema.xml

for n in {1..3}; do
  curl -XPUT $base/search/schema/searchgroup$n-schema -H 'Content-Type:application/xml' \
     --data-binary @`pwd`/schema.xml
  curl -XPUT $base/search/index/searchgroup$n-index \
    -H 'Content-Type: application/json' \
    -d "{\"schema\":\"searchgroup$n-schema\", \"n_val\":1}"
  echo "Sleeping a bit for indexes to be created"
  sleep 5
  echo "Setting Bucket Props: {\"props\":{\"search_index\":\"searchgroup$n-index\"}}"
  curl -f -XPUT $base/types/search/buckets/searchgroup$n-group/props -H "Content-Type: application/json" -d "{\"props\":{\"search_index\":\"searchgroup$n-index\"}}"
  echo "Adding 10 keys"
  for k in {1..10}; do
    curl -XPUT $base/types/search/buckets/searchgroup$n-group/keys/key$k -H "Content-Type: application/json" -d "{\"key_i\": \"$k\", \"group_i\":\"$n\"}"
  done
done

for n in {1..10}; do
  echo "Adding 10 keys"
  for k in {1..10}; do
    curl -XPUT $base/types/consistent/buckets/consistentgroup$n-group/keys/key$k -H "Content-Type: application/json" -d "{\"key_i\": \"$k\", \"group_i\":\"$n\"}"
  done
done
