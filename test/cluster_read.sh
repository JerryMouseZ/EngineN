#!/bin/bash
for i in 0 1 2 3
do
./test_sync_queue $i 9000 r $1 &
done
