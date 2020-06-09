#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
set -e

CONFIG=release
mkdir -p www/pkg

rustup target add wasm32-unknown-unknown

if [ -z "$(cargo install --list | grep wasm-pack)" ]
then
	cargo install wasm-pack
fi

if [ "${CONFIG}" = "release" ]
then
    wasm-pack build --release
else 
    wasm-pack build
fi

cd server
./gradlew run > /dev/null 2>&1 &
cd ..

cd www
npm install
npm start
