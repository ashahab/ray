# Adapted from grpc/third_party/cython.BUILD

# Adapted with modifications from tensorflow/third_party/cython.BUILD

py_library(
    name="cython_lib",
    srcs=glob(
        ["Cython/**/*.py"],
        exclude=[
            "**/Tests/*.py",
        ],
        allow_empty = True,
    ) + ["cython.py"],
    data=glob([
        "Cython/**/*.pyx",
        "Cython/Utility/*.*",
        "Cython/Includes/**/*.pxd",
    ],
    allow_empty = True,
    ),
    srcs_version="PY2AND3",
    visibility=["//visibility:public"],
)

# May not be named "cython", since that conflicts with Cython/ on OSX
filegroup(
    name="cython_binary",
    srcs=["cython.py"],
    visibility=["//visibility:public"],
    data=["cython_lib"],
)
