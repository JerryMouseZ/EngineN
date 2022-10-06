#!/bin/bash
for i in 0 1 2 3
do
./test_all $i 9000 w $1 &
done
