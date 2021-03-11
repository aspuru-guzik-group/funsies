**funsies** is a python library and execution engine to build reproducible,
composable computational workflows. 

**funsies is** 
- 🐍 DSL-free! Workflows are specified in pure python.
- 🐦 very lightweight with few dependencies.
- 🚀 super easy to deploy to compute clusters and distributed systems.
- 🔧 embedabble in your own apps.
- 📏 statically typed. You can use [mypy](http://mypy-lang.org/) to check your
  workflow scripts.

Workflows are encoded in a [redis server](https://redis.io/) and executed
using the distributed job queue library [RQ](https://python-rq.org/). A hash
tree data structure enables automatic and transparent caching and incremental
computing.

## Installation
funsies is easy to install and deploy. To install from `pip`, simply run the
following command, 
```bash
pip install git+ssh://git@github.com/aspuru-guzik-group/funsies.git@master
```
Python 3.7, 3.8 and 3.9 are supported. To run workflows, you'll need a redis
server. Redis can be installed using conda,
```bash
conda install redis
```
or pip,
```bash
pip install redis-server
```
Other redis-compatible backends such as
[fakeredis](https://pypi.org/project/fakeredis/) or
[ardb](https://github.com/yinqiwen/ardb) probably work, but are not tested.

## Hello, funsies!
funsies is fast to deploy and simple to use. To run workflows, three
components need to be connected:

- 📜 a python script describing the workflow
- 💻 a redis server that holds workflows and data
- 👷 worker processes that execute the workflow

funsies is distributed: all three components can be on different computers or
even be connected at different time. Redis is started using `redis-server`,
workers are started using `funsies worker` and the workflow is run using
python.

First, we start a redis server,
```python
$ redis-server &
```
Next, we write a little funsies "Hello, world!" script,
```python
from funsies import execute, Fun, reduce, shell
with Fun():
    # you can run shell commands
    cmd = shell('sleep 2; echo 👋 🪐')

    # and python ones
    python = reduce(sum, [3, 2])

    execute(cmd, python)
    print(f"my outputs are saved to {cmd.stdout.hash[:5]} and {python.hash[:5]}")
```
The workflow is just a normal python script,
```bash
$ python hello-world.py
my outputs are saved to 4138b and 80aa3
```
The `Fun()` context manager takes care of connections. Running this workflow
will take much less than `sleep 2` and does not greet any planets.

Workers are started using `funsies worker`,
```bash
$ funsies worker
```
Once the worker is finished, results can be obtained directly using the first
few characters of the output (like commit hashes in git),
```bash
$ funsies cat 4138b
👋 🪐
$ funsies cat 80aa3
5
```

## How does it work?

The design of **funsies** is inspired by
[git](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects) and
[ccache](https://ccache.dev/). All files and variable values are abstracted
into a provenance-tracking DAG structure. Basically, "files" are identified
entirely based on what operations lead to their creation. This (somewhat
opinionated) design produces interesting properties that are not common in
workflow engines:

#### Incremental computation

funsies automatically and transparently saves all input and output "files".
This produces automatic and transparent checkpointing and [incremental
computing](https://en.wikipedia.org/wiki/Incremental_computing). Re-running
the same funsies script, **even on a different machine**, will not perform any
computations (beyond database lookups). Modifying the script and re-running it
will only recompute changed results. 

In contrast with e.g. Make, this is not based on modification date but
directly on the data history, which is more robust to changes in the workflow.

#### Decentralized workflows

Workflows and their elements are not identified based on any global indexing
scheme. This makes it possible to generate workflows fully dynamically from
any connected computer node, to merge or compose DAGs from different databases
and to dynamically re-parametrize them.

#### No local file operations

All "files" are encoded in a redis instance, with no local filesystem
operations. This means that funsies workers can be operating without any
permanent data storage, as is often the case in containerized deployment.
This makes it possible to run heavily file-driven workflows using only a
container's [tmpfs](https://docs.docker.com/storage/tmpfs/).

## Is it production-ready?

Not really.

At this time, the funsies API is not yet stabilized. The engine and libraries
are somewhat battle-tested, have high unit test coverage and have full static
analysis. However, users should know that database dumps are not yet fully
forward- or backward-compatible.

## Related projects
funsies is intended as a lightweight alternative to industrial workflow
engines, such as [Apache Airflow](https://airflow.apache.org/) or
[Luigi](https://github.com/spotify/luigi). We rely heavily on awesome python
libraries: [RQ library](https://github.com/rq/rq),
[loguru](https://github.com/Delgan/loguru) and
[Click](https://click.palletsprojects.com/). We are inspired by
[git](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects),
[ccache](https://ccache.dev/),
[snakemake](https://snakemake.readthedocs.io/en/stable/)
[targets](https://github.com/ropensci/targets),
[rain](https://github.com/substantic/rain) and others. A comprehensive list of
other worfklow engine can be found
[here.](https://github.com/pditommaso/awesome-pipeline)
