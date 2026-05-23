#!/bin/sh

export DB_DIR=`pwd`/test-db
export WAL_DIR=$DB_DIR
export OUTPUT_DIR=$DB_DIR
export COMPRESSION_TYPE=none
export NUM_THREADS=64
export KEY_SIZE=20
export VALUE_SIZE=40
export NUM_KEYS=100000
export CACHE_SIZE=0
export DURATION=60

./benchmark.sh bulkload       # Generate data
./benchmark.sh readrandom     # Read test
./benchmark.sh overwrite      # Overwrite test (sync = 0)
./benchmark.sh updaterandom   # Update test (read first, then write, sync = 1)
```
