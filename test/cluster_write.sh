#!/bin/bash
for i in 0 1 2 3
do
./test_request $i 9000 w 5000000 &
done
