workspace(name = "com_github_ray_project_ray")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")


http_archive(
    name = "platforms",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.11/platforms-0.0.11.tar.gz",
        "https://github.com/bazelbuild/platforms/releases/download/0.0.11/platforms-0.0.11.tar.gz",
    ],
    sha256 = "29742e87275809b5e598dc2f04d86960cc7a55b3067d97221c9abbc9926bff0f",
)
http_archive(
    name = "build_bazel_rules_ios",
    sha256 = "e0dbd18f1d7a48a4b98e97dbdc45dfc7f0b1cf902afe86c442614db17f560611",
    url = "https://github.com/bazel-ios/rules_ios/releases/download/5.6.0/rules_ios.5.6.0.tar.gz",
    )

http_archive(
    name = "build_bazel_rules_apple",
    sha256 = "73ad768dfe824c736d0a8a81521867b1fb7a822acda2ed265897c03de6ae6767",
    url = "https://github.com/bazelbuild/rules_apple/releases/download/3.20.1/rules_apple.3.20.1.tar.gz",
)

load("//bazel:ray_deps_setup.bzl", "ray_deps_setup")

ray_deps_setup()


load(
    "@build_bazel_rules_ios//rules:repositories.bzl",
    "rules_ios_dependencies"
)

rules_ios_dependencies()

load(
    "@build_bazel_rules_apple//apple:repositories.bzl",
    "apple_rules_dependencies",
)

apple_rules_dependencies()

load(
    "@build_bazel_rules_swift//swift:repositories.bzl",
    "swift_rules_dependencies",
)

swift_rules_dependencies()

load(
    "@build_bazel_apple_support//lib:repositories.bzl",
    "apple_support_dependencies",
)

apple_support_dependencies()

load(
    "@com_google_protobuf//:protobuf_deps.bzl",
    "protobuf_deps",
)

protobuf_deps()

load("//bazel:ray_deps_build_all.bzl", "ray_deps_build_all")

ray_deps_build_all()

# This needs to be run after grpc_deps() in ray_deps_build_all() to make
# sure all the packages loaded by grpc_deps() are available. However a
# load() statement cannot be in a function so we put it here.
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

load("@bazel_skylib//lib:versions.bzl", "versions")

# Please keep this in sync with the .bazelversion file.
versions.check(
    maximum_bazel_version = "7.6.1",
    minimum_bazel_version = "6.5.0",
)

load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()

http_archive(
    name = "rules_python",
    sha256 = "c68bdc4fbec25de5b5493b8819cfc877c4ea299c0dcb15c244c5a00208cde311",
    strip_prefix = "rules_python-0.31.0",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.31.0/rules_python-0.31.0.tar.gz",
)

load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python3_9",
    python_version = "3.9",
    register_toolchains = False,
)

load("@python3_9//:defs.bzl", python39 = "interpreter")
load("@rules_python//python/pip_install:repositories.bzl", "pip_install_dependencies")

pip_install_dependencies()

# load("@rules_python//python:pip.bzl", "pip_parse")

# pip_parse(
#     name = "py_deps_buildkite",
#     python_interpreter_target = python39,
#     requirements_lock = "//release:requirements_buildkite.txt",
# )

# load("@py_deps_buildkite//:requirements.bzl", install_py_deps_buildkite = "install_deps")

# install_py_deps_buildkite()

register_toolchains("//:python_toolchain")

register_execution_platforms(
    "@local_config_platform//:host",
    "//:hermetic_python_platform",
)

http_archive(
    name = "crane_linux_x86_64",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "daa629648e1d1d10fc8bde5e6ce4176cbc0cd48a32211b28c3fd806e0fa5f29b",
    urls = ["https://github.com/google/go-containerregistry/releases/download/v0.19.0/go-containerregistry_Linux_x86_64.tar.gz"],
)
