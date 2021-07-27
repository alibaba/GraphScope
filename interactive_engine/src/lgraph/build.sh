#!/bin/sh

mkdir -p build
mkdir -p lib

cd build/ && rm -rf * && cmake .. && make -j && cd ..