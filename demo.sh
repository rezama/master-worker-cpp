#!/bin/bash

mkdir -p store
./cleanup.sh

# Launch three workers in background.
./masterworker worker 1 &
./masterworker worker 2 &
./masterworker worker 3 &

# Launch the master process in foreground.
./masterworker master 0

sleep 15
