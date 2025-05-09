COPTS = ["-DUSE_SSL=1"] + select({
    "@platforms//os:windows": [
        "-D_CRT_DECLARE_NONSTDC_NAMES=0",  # don't define off_t, to avoid conflicts
        "-D_WIN32",
        "-DOPENSSL_IS_BORINGSSL",
        "-DWIN32_LEAN_AND_MEAN"
    ],
    "//conditions:default": [
    ],
}) + select({
    "@//:msvc-cl": [
    ],
    "//conditions:default": [
        # Old versions of GCC (e.g. 4.9.2) can fail to compile Redis's C without this.
        "-std=c99",
    ],
})

LOPTS = select({
    "@platforms//os:windows": [
        "-DefaultLib:" + "Crypt32.lib",
    ],
    "//conditions:default": [
    ],
})

cc_library(
    name = "hiredis",
    srcs = glob(
        [
            "*.c",
            "*.h",
        ],
        exclude =
        [
            "test.c",
        ],
        allow_empty = True,
    ),
    hdrs = glob([
        "*.h",
        "adapters/*.h",
    ],
    allow_empty = True
    ),
    includes = [
        ".",
    ],
    copts = COPTS,
    linkopts = LOPTS,
    include_prefix = "hiredis",
    deps = [
        "@boringssl//:ssl",
        "@boringssl//:crypto"
    ],
    visibility = ["//visibility:public"],
)
