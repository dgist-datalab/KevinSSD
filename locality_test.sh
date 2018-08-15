#!/bin/bash

./simulator 90 0.1 | tee ./test_file/locality_90_10
sync
sleep 1
./simulator 80 0.2 | tee ./test_file/locality_80_20
sync
sleep 1
./simulator 70 0.3 | tee ./test_file/locality_70_30
sync
sleep 1
./simulator 60 0.4 | tee ./test_file/locality_60_40
sync
sleep 1
./simulator 50 0.5 | tee ./test_file/locality_50_50
sync
sleep 1

