#!/bin/bash
for i in 0 1 2 3
do
./test_request $i $1 &
done
