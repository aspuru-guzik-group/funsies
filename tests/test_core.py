"""Tests for commandline wrapper."""
# module
from funsies import Command, run, run_command, Task


# test of just simple command runner
def test_command_echo() -> None:
    """Test running the wrapper."""
    cmd = Command(executable="echo")
    results = run_command(".", None, cmd)
    assert results.returncode == 0
    assert results.stdout == b"\n"
    assert results.stderr == b""
    assert results.raises is None


def test_command_echo2() -> None:
    """Test arguments."""
    cmd = Command(executable="echo", args=["bla", "bla"])
    results = run_command(".", None, cmd)
    assert results.stdout == b"bla bla\n"
    assert results.stderr == b""
    assert results.raises is None


def test_command_environ() -> None:
    """Test environment variable."""
    cmd = Command(executable="env")
    environ = {"VARIABLE": "bla bla"}
    results = run_command(".", environ, cmd)
    assert results.raises is None
    assert results.stdout == b"VARIABLE=bla bla\n"
    assert results.stderr == b""


# def test_task_file_in() -> None:
#     """Test file inputs."""
#     cmd = Command(
#         executable="cat",
#         args=["file"],
#     )
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task([cmd], {"file": b"12345\n"})
#         results = run(cache_id, task)

#         # read
#         cache = open_cache(cache_id)
#         assert results.commands[0].stdout is not None
#         assert results.commands[0].stderr is not None
#         assert_file(get_file(cache, results.commands[0].stdout), b"12345\n")


# def test_task_file_inout() -> None:
#     """Test file input/output."""
#     cmd = Command(
#         executable="cp",
#         args=["file", "file2"],
#     )
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task([cmd], {"file": b"12345\n"}, ["file2"])
#         results = run(cache_id, task)

#         # read
#         cache = open_cache(cache_id)
#         assert_file(get_file(cache, results.outputs["file2"]), b"12345\n")


# def test_task_command_sequence() -> None:
#     """Test file outputs."""
#     cmd1 = Command(executable="cp", args=["file", "file2"])
#     cmd2 = Command(executable="cp", args=["file2", "file3"])

#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd1, cmd2],
#             inputs={"file": b"12345"},
#             outputs=["file2", "file3"],
#         )
#         results = run(cache_id, task)

#         # read
#         cache = open_cache(cache_id)
#         assert_file(get_file(cache, results.outputs["file3"]), b"12345")


# def test_cliwrap_file_err() -> None:
#     """Test file errors."""
#     cmd = Command(executable="cp", args=["file", "file2"])
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd],
#             inputs={"file": b"12345"},
#             outputs=["file3"],
#         )
#         results = run(cache_id, task)

#         # no problem
#         assert results.commands[0].raises is None

#         # but file not returned
#         assert results.outputs == {}


# def test_cliwrap_cli_err() -> None:
#     """Test command error."""
#     cmd = Command(executable="cp", args=["file2", "file3"])
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd],
#             inputs={"file": b"12345"},
#         )
#         results = run(cache_id, task)

#         assert results.commands[0].raises is None
#         assert results.commands[0].returncode > 0
#         assert results.commands[0].stdout is not None
#         assert results.commands[0].stderr is not None
#         f = get_file(open_cache(cache_id), results.commands[0].stderr)
#         assert f is not None
#         assert f != b""


# def test_command_err() -> None:
#     """Test command error."""
#     cmd = Command(executable="what is this", args=["file2", "file3"])
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd],
#             inputs={"file": b"12345"},
#         )
#         results = run(cache_id, task)

#         assert results.commands[0].raises is not None
