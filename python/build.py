#!/usr/local/bin/python3
from subprocess import check_output, check_call
import os
from glob import glob
from argparse import ArgumentParser

def get_result(cmd, env=None, **kwargs):
    return check_output(cmd, env=env, **kwargs).decode().strip()

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--sdk")
    parser.add_argument("--packages")
    parser.add_argument("--suffix")

    args = parser.parse_args()

    sdk = args.sdk
    packages = args.packages.split()
    suffix = args.suffix
    output_dir = "output"

    build_env = os.environ.copy()

    # set SDK root folder to requested SDK
    sdk_root = get_result(["xcrun", "--sdk", sdk, "--show-sdk-path"], build_env)
    build_env["SDKROOT"] = sdk_root
    # build_env["BAZEL_ARGS"] = "--toolchain_resolution_debug --target_platform_fallback=//:iPhone18 --platforms=//:iPhone18 --host_platform=//:iPhone18 --extra_execution_platforms=//:iPhone18"
    build_env["BAZEL_ARGS"] = "--toolchain_resolution_debug=@bazel_tools//tools/cpp:toolchain_type -s --sandbox_debug --ios_sdk_version=18.4 --apple_platform_type=ios --ios_multi_cpus=arm64e "
    build_env["BAZEL_ARGS"] += "--platforms=//:ios --target_platform_fallback=//:ios --host_platform=//:ios --extra_execution_platforms=//:ios "
    build_env["ARCHS"] = "arm64e"

    # use correct python executable
    python_ex = get_result(["xcrun", "-f", "python3"], build_env)
    # build_env["TARGET"] = "iphone:latest:14.0"
    build_env["CPATH"] = f"{sdk_root}/usr/include"
    build_env["C_INCLUDE_PATH"] = f"{sdk_root}/usr/include"
    build_env["CPLUS_INCLUDE_PATH"] = f"{sdk_root}/usr/include"
    # find sysconfig folder and file
    sdk_python_framework = os.path.join(sdk_root, "AppleInternal/Library/Frameworks/Python.framework")
    if not os.path.exists(sdk_python_framework) or not os.path.isdir(sdk_python_framework):
        raise RuntimeError(f"Can not find SDK python framework under {sdk_python_framework}")
    sysconfig_glob = os.path.join(sdk_python_framework, "Versions/Current/lib/python*/_sysconfigdata_*.py")
    sysconfig = glob(sysconfig_glob)
    if len(sysconfig) != 1:
        raise RuntimeError(f"Can not find python sysconfig in {sysconfig_glob}")
    sysconfig = sysconfig[0]
    build_env["_PYTHON_SYSCONFIGDATA_NAME"] = os.path.splitext(os.path.basename(sysconfig))[0]

    # Link the sysconfig to the correct location. Why this is needed, I do not know.
    # Make sure to not set the pythonpath for this
    first_valid_path_extraction = 'import sys; print([p for p in sys.path if p and not p.endswith(".zip") and not p.endswith("site-packages")][0])'
    other_sysconfig_dir = get_result([python_ex, "-c", first_valid_path_extraction], build_env)
    other_sysconfig = os.path.join(other_sysconfig_dir, os.path.basename(sysconfig))
    if not os.path.exists(other_sysconfig):
        os.symlink(sysconfig, other_sysconfig)

    # point the pythonpath to the correct sysconfig location
    build_env["PYTHONPATH"] = os.path.dirname(sysconfig)
    print(f"build_env: {build_env}")
    # As preparation, install some build-related packages
    check_call([python_ex, "-m", "pip", "install", "-U", "Cython", "wheel", "setuptools"], env=build_env)

    # Finally build the actual wheel
    # TODO: would be nice to use the created wheels for the installation already!
    # otherwise e.g. the installation of pandas will need to install numpy...
    # print(" ".join([python_ex, "-m", "pip", "wheel", "--no-binary", ":all:",
    #             "--no-deps", "-i", "https://pypi.apple.com/simple", "--trusted-host", "pypi.org", "--extra-index-url", "https://pypi.org/simple", "-w", output_dir] + packages))
    # check_call([python_ex, "-m", "pip", "wheel", "--no-binary", ":all:",
    #             "--no-deps", "-i", "https://pypi.apple.com/simple", "--trusted-host", "pypi.org", "--extra-index-url", "https://pypi.org/simple", "-w", output_dir] + packages, env=build_env)
    check_call([python_ex, "setup.py", "bdist_wheel"], env=build_env)
    # Rename the wheels to a more generic name
    for wheel in glob(os.path.join(output_dir, "*.whl")):
        wheel_name = os.path.splitext(os.path.basename(wheel))[0]
        package, version, _ = wheel_name.split("-", 2)
        wheel_name = f"{package}-{version}-{suffix}.whl"
        os.rename(wheel, os.path.join(output_dir, wheel_name))
