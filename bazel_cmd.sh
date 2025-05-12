#!/bin/bash -x
bazel build --noincompatible_disallow_ctx_resolve_tools --verbose_failures --toolchain_resolution_debug=@bazel_tools//tools/cpp:toolchain_type -s --sandbox_debug --ios_sdk_version=19.0internal --apple_platform_type=ios --ios_multi_cpus=arm64 --platforms=//:ios --config=ios --ios_minimum_os=19.0 $1
