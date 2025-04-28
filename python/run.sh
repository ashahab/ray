#!/bin/bash
# Run the script with a different user
# It seems this is necessary to pick up the right
# python environment. Not sure why.
set -x
# cp build.py /tmp/build.py
# chmod a+rwx /tmp/build.py
python3 build.py --sdk iphoneos18.4.internal --packages ray --suffix cp39-none-any
# mv /Users/local/output .