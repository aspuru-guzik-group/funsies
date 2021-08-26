"""Test the README.md example."""
# std
from os.path import join
import subprocess
import tempfile
import time

# external
import pytest


def assert_in(thing: str, readme: str) -> None:
    """Assert that a piece of text is in the readme."""
    for line in thing.splitlines():
        s = line.strip()
        if len(s) > 1:
            assert s in readme


# Starting the run
init = r"""
start-funsies \
    --no-pw \
    --workers 2
"""

# Script
script = r"""
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
run = r"""
python hello-world.py
"""
output_run = r"""
my outputs are saved to 4138b and 80aa3
"""

# Execute
execute = r"""funsies execute 4138b 80aa3"""

# cat
cat1 = ("funsies cat 4138b", "👋 🪐")
cat2 = ("funsies cat 80aa3", "5")

# Shutdown
shutdown = r"""
funsies shutdown --all
"""


@pytest.mark.slow
def test_readmemd() -> None:
    """Test README example."""
    # load readme.md
    with open("README.md", "r") as f:
        readmemd = f.read()

    with tempfile.TemporaryDirectory() as dir:
        assert_in(init, readmemd)
        start = subprocess.run(init.strip(), shell=True, cwd=dir)
        assert start.returncode == 0
        with open(join(dir, "hello-world.py"), "w") as f:
            f.write(script)

        assert_in(run, readmemd)
        assert_in(output_run, readmemd)
        rrun = subprocess.run(run, shell=True, cwd=dir, capture_output=True)
        assert rrun.returncode == 0
        assert rrun.stdout.decode().strip() == output_run.strip()

        assert_in(execute, readmemd)
        rexecute = subprocess.run(execute.strip(), shell=True, cwd=dir)
        assert rexecute.returncode == 0

        # wait for jobs to be done
        time.sleep(3)

        assert_in(cat1[0], readmemd)
        assert_in(cat1[1], readmemd)
        rcat1 = subprocess.run(cat1[0], shell=True, cwd=dir, capture_output=True)
        assert rcat1.returncode == 0
        assert rcat1.stdout.decode().strip() == cat1[1]

        assert_in(cat2[0], readmemd)
        assert_in(cat2[1], readmemd)
        rcat2 = subprocess.run(cat2[0], shell=True, cwd=dir, capture_output=True)
        assert rcat2.returncode == 0
        assert rcat2.stdout.decode().strip() == cat2[1]

        assert_in(shutdown, readmemd)
        stop = subprocess.run(shutdown.strip(), shell=True, cwd=dir)
        assert stop.returncode == 0
