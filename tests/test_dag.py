"""Test of Funsies shell capabilities."""
# external
from fakeredis import FakeStrictRedis as Redis
import networkx as nx
import rq
from rq import Queue

# module
from funsies import dag
from funsies import take, morph, put, shell


def test_dag() -> None:
    """Test DAG."""
    db = Redis()
    dat = put(db, "bla bla")
    step1 = morph(db, dat, lambda x: x.decode().upper().encode())
    step2 = shell(db, "cat file1 file2", inp=dict(file1=step1, file2=dat))
    output = step2.stdout

    d = dag.get_dag(db, output.hash)
    assert len(d.nodes) == 3
    assert len(d.edges) == 2
    assert nx.is_directed_acyclic_graph(d)


# process a dag
import redis

db = redis.Redis()
dat = put(db, "bla bla")
step1 = morph(db, dat, lambda x: x.decode().upper().encode())
step2 = shell(db, "sleep 3.0; cat file1 file2", inp=dict(file1=step1, file2=dat))
output = step2.stdout
# q = Queue(connection=db, default_timeout=-1, is_async=False)
q = Queue(connection=db, default_timeout=-1)

# get dag
d = dag.get_dag(db, output.hash)

# We start a run for each node that has no dependencies
for n in d.nodes:
    if len(d.pred[n]) == 0:
        q.enqueue_call(dag.rq_eval, args=(n, d))


import time

for i in range(100):
    out = take(db, output)
    time.sleep(0.3)
    if out is not None:
        print(out)
        print(i)
        break
