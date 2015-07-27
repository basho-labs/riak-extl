#!/bin/bash

killall beam.smp
for n in {3..4}; do
	rm -r -f dev$n/data
	mkdir -p dev$n/data
done
