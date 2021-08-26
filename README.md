# funsies
is a python library and execution engine to build reproducible,
fault-tolerant, distributed and composable computational workflows.

- 🐍 Workflows are specified in pure python.
- 🐦 Lightweight with few dependencies.
- 🚀 Easy to deploy to compute clusters and distributed systems.
- 🔧 Can be embedded in your own apps.
- 📏 First-class support for static analysis. Use
  [mypy](http://mypy-lang.org/) to check your workflows!

Workflows are encoded in a [redis server](https://redis.io/) and executed
using the distributed job queue library [RQ](https://python-rq.org/). A hash
tree data structure enables automatic and transparent caching and incremental
computing.

[Source docs can be found
here.](https://aspuru-guzik-group.github.io/funsies/) Some example funsies
scripts can be found in the [recipes folder.](./recipes)

## Installation
Using `pip`, 

```bash
pip install funsies
```

This will enable the `funsies` CLI tool as well as the `funsies` python
module. Python 3.7, 3.8 and 3.9 are supported. To run workflows, you'll need a
Redis server, version 4.x or higher. On Linux Redis can be installed using conda,

```bash
conda install redis
```

pip,

```bash
pip install redis-server
```

or your system package manager. On Mac OSX, Redis can be downloaded using
Homebrew,

```bash
brew install redis
```

(Windows is not supported by Redis, but a third-party package can be obtained
from [this repository](https://github.com/tporadowski/redis). This has **not**
been tested, however.)

## Hello, funsies!
To run workflows, three components need to be connected:

- 📜 a python script describing the workflow
- 💻 a redis server that holds workflows and data
- 👷 worker processes that execute the workflow

funsies is distributed: all three components can be on different computers or
even be connected at different time. Redis is started using `redis-server`,
workers are started using `funsies worker` and the workflow is run using
python.

For running on a single machine, the `start-funsies` script takes care of starting the database and workers,

```bash
start-funsies \
    --no-pw \
    --workers 2
```

Here is an example workflow script,

```python
from funsies import Fun, reduce, shell
with Fun():
    # you can run shell commands
    cmd = shell('sleep 2; echo 👋 🪐')
    # and python ones
    python = reduce(sum, [3, 2])
    # outputs are saved at hash addresses
    print(f"my outputs are saved to {cmd.stdout.hash[:5]} and {python.hash[:5]}")
```

The workflow is just python, and is run using the python interpreter,

```bash
$ python hello-world.py
my outputs are saved to 4138b and 80aa3
```

The `Fun()` context manager takes care of connecting to the database. The
script should execute immediately; no work is done just yet because workflows
are lazily executed.

To execute the workflow, we trigger using the hashes above using the CLI,

```bash
$ funsies execute 4138b 80aa3
```

Once the workers are finished, results can be printed directly to stdout using
their hashes,

```bash
$ funsies cat 4138b
👋 🪐
$ funsies cat 80aa3
5
```

They can also be accessed from within python, from other steps in the
workflows etc. Shutting down the database and workers can also be performed
using the CLI,

```bash
$ funsies shutdown --all
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
and to dynamically re-parametrize them, etc.

#### No local file operations

All "files" are encoded in a redis instance or to a data directory, with no
local filesystem management required. funsies workers can even operate without
any permanent data storage, as is often the case in file-driven workflows
using only a container's [tmpfs](https://docs.docker.com/storage/tmpfs/).

## Recovering from failures

Raised exceptions in python codes, worker failures, missing output files and
other error conditions are automatically caught by funsies workers, providing
fault tolerance to workflows. Errors are logged on `stderr` with full
traceback and can be recovered from the database.

Steps that depend on failed ones propagate those
errors and their provenance. Errors can then be dealt with wherever it is most
appropriate to do so [using techniques from functional
programming.](https://fsharpforfunandprofit.com/rop/) 

As an example, consider a workflow that first runs a CLI program `simulate`
that ought to produce a `results.csv` file, which is subsequently analyzed
using a python function `analyze_data()`,

```python
import funsies as f

sim = f.shell("simulate data.inp", inp={"data.inp":"some input"}, out=["results.csv"])
final = f.reduce(analyze_data, sim.out["results.csv"])
```

In a normal python program, `analyze_data()` would need to guard against the
possibility that `results.csv` is absent, or risk a fatal exception. In the
above funsies script, if `results.csv` is not produced, then it is replaced by
an instance of `Error` which tracks the failing step. The workflow engine
automatically shortcircuit the execution of `analyze_data` and insteads
forward the `Error` to `final`. In this way, the value of `final` provides
direct error tracing to the failed step. Furthermore, it means that
`analyze_data` does not need it's own error handling code if its output is
optional or if the error is better dealt with in a later step.

This error-handling approach is heavily influenced by the `Result<T,E>` type
from [the Rust programming language.](https://doc.rust-lang.org/std/result/)


## Is it production-ready?

🧪 warning: funsies is research-grade code ! 🧪

At this time, the funsies API is fairly stable. However, users should know
that database dumps are not yet fully forward- or backward-compatible, and
breaking changes are likely to be introduced on new releases.

## Related projects
funsies is intended as a lightweight alternative to industrial workflow
engines, such as [Apache Airflow](https://airflow.apache.org/) or
[Luigi](https://github.com/spotify/luigi). We rely heavily on awesome python
libraries: [RQ library](https://github.com/rq/rq),
[loguru](https://github.com/Delgan/loguru),
[Click](https://click.palletsprojects.com/) and
[chevron](https://github.com/noahmorrison/chevron). We are inspired by
[git](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects),
[ccache](https://ccache.dev/),
[snakemake](https://snakemake.readthedocs.io/en/stable/)
[targets](https://github.com/ropensci/targets),
[rain](https://github.com/substantic/rain) and others. A comprehensive list of
other worfklow engine can be found
[here.](https://github.com/pditommaso/awesome-pipeline)


## License

funsies is provided under the MIT license.

## Contributing

All contributions are welcome! Consult [the CONTRIBUTING](./CONTRIBUTING.md)
file for help. Please file issues for any bugs and documentation problems.
