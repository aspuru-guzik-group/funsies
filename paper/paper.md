---
title: 'funsies: A minimalist, distributed and dynamic workflow engine'
tags:
  - workflow
  - Python
  - redis
  - decentralized
  - computational chemistry
authors:
  - name: Cyrille Lavigne^[Corresponding author.]
    orcid: 0000-0003-2778-1866
    affiliation: 1
  - name: Alán Aspuru-Guzik
    orcid: 0000-0002-8277-4434
    affiliation: "1, 2, 3, 4"
affiliations:
  - name: Department of Computer Science, University of Toronto, 40 St. George St, Toronto, Ontario M5S 2E4, Canada
    index: 1
  - name: Chemical Physics Theory Group, Department of Chemistry, University of Toronto, 80 St. George St, Toronto, Ontario M5S 3H6, Canada
    index: 2
  - name: Vector Institute for Artificial Intelligence, 661 University Ave Suite 710, Toronto, Ontario M5G 1M1, Canada
    index: 3
  - name: Lebovic Fellow, Canadian Institute for Advanced Research (CIFAR), 661 University Ave, Toronto, Ontario M5G, Canada
    index: 4
date: 27 April 2021
bibliography: paper.bib
---

# Summary

Large-scale, high-throughput computational investigations are increasingly
common in chemistry and physics. Until recently, computational chemistry was
primarily performed using all-in-one monolithic software
packages.[@smith:2020; @aquilante:2020; @kuhne:2020;
@apra:2020; @barca:2020; @romero:2020] However, the
limits of individual programs become evident when tackling complex
multifaceted problems. As such, it is increasingly common to use multiple
disparate software packages in a single computational pipeline,
[@pollice:2021] often stitched together using shell scripts in
languages such as Bash, or using Python and other interpreted languages.

These complex computational pipelines are difficult to scale and automate, as
they often include manual steps and significant “human-in-the-loop” tuning.
Shell scripting errors are often undetected, which can compromise
scientific results. Conversely, exception-based error handling, the standard
approach in Python, can readily bring a computational workflow to a halt when
exceptions are not properly caught.[@weimer:2008]

`funsies` is a set of python programs and modules to describe, execute and
analyze computational workflows, with first-class support for shell scripting.
It includes a lightweight, decentralized workflow engine backed by a NoSQL
store.[@redis] Using `funsies`, external program and python-based computations
are easily mixed together. Errors are detected and propagated throughout
computations. Automatic, transparent incremental computing (based on a hash
tree data structure) provides a convenient environment for iterative
prototyping of computationally expensive workflows.

# Statement of need


Modern workflow management programs used in the private sector, such as Apache
Airflow[@airflow] and Cadence[@cadence], are robust and extremely scalable,
but are difficult to deploy. Scientific workflow management systems, such as
Snakemake and others,[@mlder_sustainable_2021] are easier to set up on
high-performance computing clusters, but are tuned to the needs of specific
disciplines, such as bioinformatics or machine learning. This includes, for
example, the use of configuration file formats (YAML, JSON, etc.), packaging
tools (for example, conda or Docker), locked-in compute providers (Amazon Web
Services, Google Cloud) and storage formats that may be common in specific
scientific fields but not throughout the greater community.

For our own group's research program, we wanted to have available a
lightweight workflow management system that could be readily deployed to new
and varied computational facilities and local workstations with minimal
effort. This system had to support our existing shell-based and Python-based
scripts, and be flexible enough for rapid prototyping all the way to
large-scale computational campaigns, and provide an embeddable solution that
can be bundled within other software.[@lavigne_automatic_2020] Finally, we
were looking for a tool that could integrate data generation and storage, to
avoid the common practice of transforming the filesystem into what is
effectively a schema-less database. We developed `funsies` to address those
needs.


# Features and Implementation

`funsies` is a Python library and a set of associated command-line tools.
Using the `funsies` library, general computational workflows are described in
lazily evaluated Python code. Operations in `funsies` are taken to be pure,
that is, all operation outputs are entirely and solely determined by their
inputs. Workflows are orchestrated using python by manipulating pointers to
yet-to-be-calculated data. Workflow instructions are transparently translated
and saved as graph elements in a Redis database.

Computational evaluation is initiated by the user asking for specific output
value. The task graph from these final outputs is walked back all the way to
those operations with no dependencies. These initial operations are then
queued for execution. Lightweight worker processes, instantiated from the
command line on local or remote machines, connect to the Redis database and
start executing the workflow. For each operation, the worker checks if outputs
are already cached, and if not, executes the associated function and saves its
outputs. It then enqueues any dependents for execution, by itself or by other
workers. In this way, the entire computational graph is evaluated in a
distributed, decentralized fashion without any scheduler or manager program.
Errors in workflows are handled using a functional approach inspired by
Rust.15 Specifically, exceptions are propagated through workflow steps,
canceling dependent tasks, without interrupting valid workflow branches. This
provides both easy error tracing and a high degree of fault tolerance.


The main distinguishing feature of `funsies` is the hash tree structure that
is used to encode all operations and their inputs. The causal hashing approach
used in `funsies` can also be found in Snakemake[@mlder_sustainable_2021] as
an optional component and the (now defunct) Koji workflow
system,[@maymounkov_koji_2018] as part of the Nix package
manager[@dolstra_nix_2004] and in the Git version control
system.[@chacon_pro_2014] In `funsies`, we replace all filesystem operations
with hash addressed operations; that is all I/O operations and dependencies
are tracked.

Every operation has a hash address that is computed from the hash values of
its dependencies and a hashed identifier for the associated operation on data.
In this way, the consistency of data dependencies is strongly enforced.
Changes to data and operations are automatically and transparently propagated,
as changing a single dependency will cause a rehash of all its dependents,
effectively producing a new workflow with no associated data that needs to be
recomputed. Alternatively, if data already exists at a specific hash address,
then it was generated from the same operations that produced that hash. In
this way, the hash tree structure enables transparent and automatic
incremental recomputing. 

Using hash addresses also enables decentralization, as we can rely on the
unlikeliness of hash collisions[@stevens_first_2017] to eliminate centralized
locks. An important advantage of this approach is that it allows worker
processes to generate their own workflows of tasks dynamically. Results from
these dynamic workflows can be collected and used further in the workflow
description, provided they can be reduced to a number of outputs known at
compile time, a technique similar to MapReduce.[@dean_mapreduce_2004]

As of now, we have published one project7 that used an earlier iteration of
`funsies`, and are using it in multiple ongoing inquiries. We provide
several sample workflows on Github, with a focus on computational
chemistry, quantum computing, and high-performance computing infrastructure.

We intend to maintain `funsies` and of course welcome collaborations from
contributors around the world.


# Acknowledgements

We acknowledge testing by early users Cher-Tian Ser (@chertianser), Kjell
Jorner (@kjelljorner) and Gabriel dos Passos Gomes (@gabegomes). CL also
thanks Chris Crebolder (@ccrebolder) for help setting up documentation pages.
We acknowledge the Defense Advanced Research Projects Agency (DARPA) under the
Accelerated Molecular Discovery Program under Cooperative Agreement No.
HR00111920027 dated August 1, 2019. The content of the information presented
in this work does not necessarily reflect the position or the policy of the
Government. A. A.-G. thanks Dr. Anders G. Frøseth for his generous support. A.
A.-G. also acknowledges the generous support of Natural Resources Canada and
the Canada 150 Research Chairs program. We thank Compute Canada for providing
computational resources.

# References
