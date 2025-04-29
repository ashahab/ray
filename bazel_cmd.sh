#!/bin/bash -x
bazel build --verbose_failures --toolchain_resolution_debug=@bazel_tools//tools/cpp:toolchain_type -s --sandbox_debug --ios_sdk_version=18.4internal --apple_platform_type=ios --ios_multi_cpus=arm64e --platforms=//:ios --config=ios --ios_minimum_os=18.4 $1
