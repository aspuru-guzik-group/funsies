**funsies** is a lightweight library and engine to build reproducible,
composable computational workflows. It is easy to deploy and embed in user
applications.

Workflows are described entirely in python, encoded in a [redis
server](https://redis.io/) and executed using the distributed job queue
library [RQ](https://python-rq.org/). A hash tree data structure enables
automatic and transparent caching and incremental computing. 


<!-- Funsies is built to be simple to use. First, it needs a redis server, which -->
<!-- can be locally installed in anaconda, -->

<!--     conda install -c anaconda redis -->
<!--     redis-server -->

<!-- Then, the workflow can be built and registered in the Redis database entirely -->
<!-- in python, -->

<!--     from funsies import shell, Fun -->
    
<!--     # load some parameters -->
<!--     with open('params.in','rb') as f: -->
<!--         parameters = f.read() -->
    
<!--     with Fun(): -->
<!--         t1 = shell("qchem params.in", -->
<!--                    inp={"file.in": parameters}, out=["params.out"]) -->

<!-- `shell()` make simple shell commands with explicit input and output files. All -->
<!-- input and output files are automatically saved in Redis. The `Fun(connection)` -->
<!-- context sets up the connection to Redis (by default, on localhost). -->

<!-- Chaining shell commands is simple, -->

<!--     t2 = shell( -->
<!--         'grep "HOMO-LUMO ENERGY" myfile', -->
<!--         inp={"myfile": t1.out["params.out"]}, -->
<!--     ) -->

<!-- The outputs from a call to `shell()` can immediately be used as inputs to other -->
<!-- tasks, even as none of the shell commands have even started computing. This is -->
<!-- because the "files" are not actual populated values but pointers to (currently -->
<!-- absent) data on the redis server. -->

<!-- Python can also be used to do computations. Some user-friendly wrappers are -->
<!-- provided in `reduce()` and `morph()`. Here we apply `a_python_function` -->
<!-- directly to the output of grep above, -->

<!--     def uncap(arg: bytes) -> bytes: -->
<!--         return arg.decode().lower().encode() -->
    
<!--     from funsies import morph, take -->
<!--     t3 = morph(uncap, t2.stdout) -->

<!-- The result in `t3` is now in lowercase. Once the workflow has been executed, it -->
<!-- becomes retrievable using `take().` -->


<!-- <a id="orgcca739e"></a> -->

<!-- ## Running a workflow -->

<!-- Running workflows is done using rq, a minimalist job queue library. To create -->
<!-- workers, simply run the following in the shell, -->

<!--     funsies worker -->

<!-- There are many settings available. Importantly, workers can connect to remote -->
<!-- Redis servers, which allow full distributed computations. An example as to how -->
<!-- to do this on a SLURM server (common in HPC) is shown in -->
<!-- `examples/slurm_submit.sh`. -->

<!-- Once a workflow is setup in python, the `execute` function applied to any of the -->
<!-- workflow's object (from `shell()` etc.) will enqueue the entire DAG required to -->
<!-- compute the object. It's as simple as doing, -->

<!--     # still in the same context. -->
<!--     execute(t3) -->

<!-- This will enqueue all the required jobs in the Redis Queue. Note that no -->
<!-- dependency that has already been computed is ever recomputed. Thus, `execute()` -->
<!-- can be used to re-run only data analysis routines (that change often) without -->
<!-- re-running any simulations. -->


<!-- <a id="org7b170a6"></a> -->

<!-- ## Results, memoization and persistence -->

<!-- The major advantage of using funsies is that it automatically and -->
<!-- transparently saves all **marked** input and output "files". This memoization -->
<!-- enables automatic checkpointing and incremental computing. -->

<!-- Following on the example above, re-running the same script, **even on a -->
<!-- different machine**, will not perform any computations (beyond database -->
<!-- lookups). Modifying the script and re-running it will only recompute changed -->
<!-- results. This means, for example, that if we want to change slightly the final -->
<!-- data outputs of an expensive computation, we can do so entirely out of the -->
<!-- cluster. We only ever need to carry around two files: the database dump and -->
<!-- the computation script.  -->

<!-- To go back to our example, we can get the result from `t3` by pulling it's -->
<!-- output file once the computation is done. For example, we could `scp` the -->
<!-- database `dump.db` file to a local machine, start redis, and re-run the entire -->
<!-- script with only this line added, -->

<!--     from funsies import take -->
<!--     with Fun(): -->
<!--         print(take(t3)) -->

<!-- to print the result from `t3`. If we additionally wanted to inspect the stdout -->
<!-- from t1, we could add this at the end, -->

<!--     print(take(t1.stdout)) -->

<!-- No expensive computations are performed in either case. -->


<!-- <a id="org05b4b8a"></a> -->

<!-- # Why not *x* ? -->

<!-- (where *x* ∈ S, [awesome pipelining](https://github.com/pditommaso/awesome-pipeline) ∪ [workflow codes](https://github.com/meirwah/awesome-workflow-engines) ⊂ S) -->

<!-- I've created funsies because I wanted a pipelining code that is minimal, -->
<!-- typed, deployable on HPC resources (not dependent on docker, AWS, etc.) and -->
<!-- (most importantly) with **reproducible, persistent memoization**. -->

<!-- Funsies is specifically built for the kind of workflows common in -->
<!-- computational chemistry. It is most similar to [reflow](https://github.com/grailbio/reflow) and [snakemake](https://snakemake.readthedocs.io/en/stable/), albeit in -->
<!-- python instead of Go, and significantly simpler (and less robust / featureful -->
<!-- of course). -->

<!-- -   **Single source of truth**: In funsies, the script that generates the data also -->
<!--     describes the data. While keeping code and data tightly coupled is often -->
<!--     frowned upon, it ensures that there is no documentation that will go out of -->
<!--     date or lab notebooks that are more "post-it notes on a board" than -->
<!--     "notebook". -->
<!-- -   **Few but expensive**: Funsies assumes that tasks are few but that they are very -->
<!--     expensive to compute. It is designed for workflow with 100s ⨉ 40 core hour -->
<!--     jobs (like optimizing molecular geometries) not workflows with 100,000 ⨉ 10 -->
<!--     core second jobs, as may be present in large scale data analytics. -->
<!-- -   **Run anywhere**: Academic research is always severely financially constrained, -->
<!--     and computational chemistry software is often site-locked. Containerization -->
<!--     (like Docker) is still slowly coming into the HPC sphere. Funsies is built -->
<!--     so that it can run anywhere without root access. -->
<!-- -   **Minimal setup and interface**: Although full-scale workflow software is -->
<!--     obviously more robust, it is also much too cumbersome to setup. Similarly, -->
<!--     extensive design of database schema is too unwieldy, even if it is by far -->
<!--     the better solution. Funsies target instead the "file-driven databases" used -->
<!--     by academics that rapidly become unreadable, non-backed up messes -->
<!--     (`expt_2020/jun/ParameterSearch3/alpha=0point3.csv`) -->


<!-- <a id="org6db8173"></a> -->

<!-- # Architecture -->


<!-- <a id="orgd1abd08"></a> -->

<!-- ## Hash-based graph -->

<!-- Funsies stores all shell commands and python functions as values in redis -->
<!-- store, with keys given by hashing a set of invariants. For commmand-line -->
<!-- tasks, these are: -->

<!-- -   Input file hashes (unordered) -->
<!-- -   Output file names (unordered) -->
<!-- -   Shell commands -->

<!-- For python functions, the invariants are: -->

<!-- -   Input file hashes -->
<!-- -   Number of outputs -->
<!-- -   The name of the function -->

<!-- (Although cloudpickle is used to call python functions, the function name is -->
<!-- used to generate the address hash, as the pickle form is python version -->
<!-- dependent.) -->

<!-- Files with explicitly given content are hashed based on this content, while -->
<!-- files generated as outputs to other commands are only hashed based on the hash -->
<!-- of the generator. -->

<!-- This structure is analoguous to that of a blockchain (but as a directed -->
<!-- acyclic graph). Like a blockchain, it has the advantage that any modification -->
<!-- to the chain is immediately and automatically detectable as it yields -->
<!-- completely different hashes for all descending "blocks". Using this -->
<!-- architecture, we get transparent caching and incremental recomputation of -->
<!-- tasks and their dependent tasks. -->


<!-- <a id="org879a504"></a> -->

<!-- ## No filesystem -->

<!-- Funsies "files" are always artefacts in the database. This abstraction is -->
<!-- enforced by running all commandline tasks entirely in temporary directories. -->
<!-- Any files not explicitly saved as an output is **deleted**. -->

<!-- This is obviously a very opinionated design choice, but it is also one that -->
<!-- enables the caching scheme used by funsies. Indeed, by completely removing any -->
<!-- direct file management, we can ensure that **all file-like objects** are accounted -->
<!-- for at every point in incremental calculations, with no side-effects. I should -->
<!-- note that this means that "restart" files must be explicitly accounted for by -->
<!-- the user. -->

<!-- By completely abstracting away the filesystem, we ensure that every generated -->
<!-- result is fully specified within the calculation workflow. -->


<!-- <a id="orgeb10501"></a> -->

<!-- ## "Stateless" python code -->

<!-- All computation state is stored in the Redis instance. This is critical in -->
<!-- that it enables fully automatic checkpointing and remove the need for -->
<!-- communication between nodes. -->


<!-- <a id="orgf87d6e1"></a> -->

<!-- # Extras -->


<!-- <a id="org11c67ac"></a> -->

<!-- ## Dashboard -->

<!-- Currently running jobs can be inspected using [rq-dashboard](https://github.com/Parallels/rq-dashboard). For HPC, this most -->
<!-- readily done using a ssh tunnel. On a specific node with access to the Redis -->
<!-- server (`${cluster_node}` below), run the dashboard using -->

<!--     rq-dashboard -u redis://${redis_server_url}:$port -->

<!-- On the local machine, run -->

<!--     ssh -N -f -L 9181:${cluster_node}:9181 ${cluster_address} -->

<!-- to tunnel to the dashboard. If everything worked, the dashboard should be -->
<!-- accessible using a browser pointed at address <http://localhost:9181> -->


<!-- <a id="org76515b4"></a> -->

<!-- ## Using funsies to orchestrate jobs -->

<!-- One potentially interesting use of funsies is as an HPC job manager or -->
<!-- orchestration tool. Due to its distributed nature, the submitting and result -->
<!-- analysis script does not need to be local to the workers. -->

<!-- In HPC environment, the easiest way to do this orchestration is through using -->
<!-- a ssh tunnel. In debug mode, this would look like -->

<!--     salloc -p debug --time=1:00:00 -->
<!--     module load conda3 -->
<!--     source activate funsies         # the same env should be used for submission -->
<!--                                     # if any python code is to be run on the -->
<!--                                     # workers. -->
    
<!--     redis-server redis.conf &       # make sure to use a redis.conf file that -->
<!--                                     # disables protected mode. -->
    
<!--     # start workers -->
<!--     srun --ntasks=40 --cpus-per-task=1 funsies worker -->

<!-- Then, we just need to punch a ssh tunnel to run jobs on the scinet node above, -->

<!--     ssh niagara.scinet.utoronto.ca -L 6379:{node_address}.local:6379 -->

<!-- where `{node_address}` is the specific nodes that run the redis instance. This -->
<!-- approach can be scaled to many more than one node of course. In addition, once -->
<!-- jobs are executing, the client does not need to remain connected anymore, -->
<!-- which provides a degree of stability. -->

