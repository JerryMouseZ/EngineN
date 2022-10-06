#!/bin/bash
for i in 0 1 2 3
do
./test_backup $i 11000 r 50000000 &
done
