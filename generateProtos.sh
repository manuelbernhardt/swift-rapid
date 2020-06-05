#!/bin/sh

cd Protos
protoc --swift_opt=Visibility=Public --swift_out=../Sources/SwiftRapid rapid.proto
cd ..