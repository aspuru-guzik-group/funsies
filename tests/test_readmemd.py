"""Test the README.md example."""
import subprocess
import tempfile

# Starting the run
init = """
start-funsies output_directory \
    --no-password \
    --workers 2 \
"""

# Script
script = """
from funsies import Fun, reduce, shell
with Fun():
    # you can run shell commands
    cmd = shell('sleep 2; echo 👋 🪐')
    # and python ones
    python = reduce(sum, [3, 2])
    # outputs are saved at hash addresses
    print(f"my outputs are saved to {cmd.stdout.hash[:5]} and {python.hash[:5]}")
"""


# Run and outputs
run = """
python hello-world.py
"""
output_run = """
my outputs are saved to 4138b and 80aa3
"""

# Shutdown
shutdown = """
funsies shutdown --all
"""


with tempfile.TemporaryDirectory() as dir:
    start = subprocess.Popen(init.strip(), shell=True)
