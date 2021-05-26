# How to contribute to funsies

funsies is a free software project, and we welcome every kind of contributions
(documentations, bug reports, fixes, etc.)

If you encounter an issue, please fill a report [using the github Issues
page.](https://github.com/aspuru-guzik-group/funsies/issues)

If you have a fix, use the [pull request functionality on
Github.](https://github.com/aspuru-guzik-group/funsies/pulls) Make sure your
commits are given concise, explanatory names and limit your file edits to the
minimal relevant parts. Always run formatters before comitting (as described
below).

## Instructions for contributors

This repo has a CI workflow that will run the test suite, mypy, linting etc.,
and all tests and lints should pass. If you want to run CI locally before
pushing, all of it is [automated using Nox for local
development.](https://nox.thea.codes/en/stable/) To install nox in a python
env, use

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

Note that nox will take care of installing packages from PyPI for each of the
above steps, so you shouldn't need to do anything besides installing nox
itself to get a working dev environment.

funsies is formatted using [black](https://github.com/psf/black) and
[isort](https://pypi.org/project/isort/), which are run automatically using
nox, as described above.

Importantly, **funsies is a statically typed program,** and every function
boundaries needs annotations. 

Note also that the code tries to maintain (as much as possible) a functionally
pure paradigm for operations encoded in the database. This means that adding
new functionality can be somewhat tricky. If you find yourself meddling with
the internals described below, please talk to me (@clavigne) first!


## Internals: how to read this code

funsies is not a particularly complex codebase (yet) and should be fairly
readable. Here is a basic summary of the internal architecture.

The low-level workflow generation and encoding can be found in
[_graph.py](https://github.com/aspuru-guzik-group/funsies/blob/master/src/funsies/_graph.py)
 and
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
 

