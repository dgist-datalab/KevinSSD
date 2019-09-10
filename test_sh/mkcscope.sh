#!/bin/sh
rm -rf cscope.files cscope.files

find . −name′∗.c′−o−name′∗.cpp′−o−name′∗.cc′−o−name′∗.h′−o−name′∗.s′−o−name′∗.S′ -print > cscope.files

cscope -i cscope.files

