#! /bin/bash
sudo ifconfig enp1s0 down
sudo ifconfig enp1s0 mtu 128
sudo ifconfig enp1s0 up
