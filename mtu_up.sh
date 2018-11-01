#! /bin/bash
sudo ifconfig enp2s0f0 down
sudo ifconfig enp2s0f0 mtu $1
sudo ifconfig enp2s0f0 up
