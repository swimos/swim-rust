#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
set -e

cd server
./gradlew run > /dev/null 2>&1 &
cd ..

cargo run