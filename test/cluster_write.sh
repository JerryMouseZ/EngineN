#!/bin/bash
./test_sync_queue 0 9000 $1 &
./test_sync_queue 1 9000 $1 &
./test_sync_queue 2 9000 $1 &
./test_sync_queue 3 9000 $1