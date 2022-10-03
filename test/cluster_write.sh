#!/bin/bash
for i in 0 1 2 3
do
./test_local $i w $1 &
done
