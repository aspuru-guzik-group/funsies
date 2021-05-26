# How to contribute to funsies

funsies is a free software project, and we welcome every kind of contributions
(documentations, bug reports, fixes, etc.)

If you encounter an issue, please fill a report [using the github Issues
page.](https://github.com/aspuru-guzik-group/funsies/issues)

If you have a fix, use the [pull request functionality on
Github.](https://github.com/aspuru-guzik-group/funsies/pulls). Make sure your
commits are named properly.

## Tests and lints

This repo has a CI workflow that will run the test suite, mypy, linting etc.,
and all tests and lints should pass. If you want to test it before pushing to
the CI, all of this is [automated using
Nox.](https://nox.thea.codes/en/stable/) To install nox and the development
packages in a python env, use

```bash
pip install nox
```

To automatically format code so that it passes linting, you can use

```bash
nox -rs fmt
```

To lint the code and run the mypy type checker, use

```bash
nox -rs lint
nox -rs mypy
```

Tests can be run using `nox -rs tests` which will run tests for three python
versions (if they are installed). Some of the tests are rather slow and so are
skipped by default, you can run the whole set using `nox -rs tests -- --cov`.
You'll need a redis server installed and available on `$PATH`.

## Code conventions and best practices

funsies is formatted using [black](https://github.com/psf/black) and
[isort](https://pypi.org/project/isort/), which are run automatically using
nox, as described above.

Importantly, funsies is (as much as possible in python) a statically typed
code, and every function boundaries needs annotations. In addition, because
funsies is used to simulate large, multi-node workflows, raising exceptions
should be avoided as much as possible.

And note that (critically) the code tries to maintain (as much as possible) a
functionally pure paradigm for operations that are not encoded in the
database. The lower-level architecture of this system is coded in
[_graph.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_graph.py)
and should not be much of a concern if you don't touch this code.


## Internals: how to read this code

funsies is not a particularly complex codebase (yet) and should be fairly
readable. Here is a basic summary of the internal architecture.

The low-level workflow generation and encoding can be found in
[_graph.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_graph.py)and
[_funsies.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_funsies.py).
The former contains hash calculation, getters and setters for data and
dependencies, while the latter contains data structure for encoding
operations. Data is serialized in
[_serdes.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_serdes.py)

At the lowest level, workflow execution uses runner functions registered in
[_run.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_run.py)
and called from `_run.run_op()`. The correct ordering of executions (as well
as other graph traversal functions) are computed in
[_dag.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_dag.py).

User functions are found in
[ui.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/ui.py),
[fp.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/fp.py)
and others for shell operations, python operations etc. Console entry points
(the `funsies` cli command) are grouped in
[_cli.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_cli.py).
 

