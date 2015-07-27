#!/bin/bash

find ./ -name riak.conf -exec perl -pi -e 's/#* *search = off/search = on/' {} \;
