#!/bin/bash
echo "Building wheel"
which python3
which pip3
sudo python3 -m build
echo "Reinstalling wheel (local)"
sudo pip install --force-reinstall dist/xfarm_monitoring-1.1.0a0-py3-none-any.whl
echo "Finished."