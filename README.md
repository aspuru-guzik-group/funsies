**funsies** is a library and engine to build reproducible,
composable computational workflows.
- 🐍 funsies workflows are entirely in python.
- 🪶 lightweight library with few dependencies.
- 🚀 easy to deploy.
- 🔧 embedabble in your own apps.

Workflows are encoded in a [redis server](https://redis.io/) and executed
using the distributed job queue library [RQ](https://python-rq.org/). A hash
tree data structure enables automatic and transparent caching and incremental
computing.

## Installation
`funsies` is easy to install and deploy. To install from `pip`, simply run the
following command, 
```bash
pip install git+ssh://git@github.com/aspuru-guzik-group/funsies.git@master
```
Python 3.8 and 3.9 are supported. To run workflows, you'll need a redis
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
funsies is easy to deploy and simple to use. To run workflows, three
components need to be connected:

- 📜 a python script describing the workflow
- 💻 a redis server that holds workflows and data
- 👷 worker processes that execute the workflow

funsies is distributed: all three components can be on different computers or
even be connected at different time. Redis is started using `redis-server`,
workers are started using `funsies worker` and the workflow is run using
python.

Here is a funsies "Hello, world!" script,
```python
import funsies as fun
with fun.Fun():
    cmd = fun.shell('sleep 2; echo')
    fun.execute(cmd)
    print(f"my output is {cmd.stdout.hash[:5]}")
```
The python script should be run while connected to redis. For a local
installation,
```bash
$ redis-server &
$ python hello-world.py
my output is c2a8f
```
The `Fun()` context manager takes care of connections. Running this workflow
will take much less than `sleep 2` and does not greet any planets.

Workers are started using `funsies worker`,
```bash
$ funsies worker
```
Once the worker is finished, results can be obtained directly using the first
few characters of the output hash,
```bash
$ funsies cat c2a8f
👋 🪐
```

## Results, memoization and persistence

One major advantage of using funsies is that it automatically and
transparently saves all input and output "files", which enables automatic
checkpointing and [incremental
computing](https://en.wikipedia.org/wiki/Incremental_computing).

Re-running the same funsies script, **even on a different machine**, will not
perform any computations (beyond database lookups). Modifying the script and
re-running it will only recompute changed results. This is achieved using a
hash tree structure that transparently tracks the history of all data produced
and consumed.

**funsies completely abstract all filesystem operations.** Funsies "files" are
all stored in the database. This abstraction is enforced by running all
commandline tasks entirely in temporary directories. Any files not explicitly
saved as an output is **deleted**. By completely abstracting away the
filesystem, we ensure that every generated result is fully specified within
the calculation workflow.



