"""Test of Funsies shell capabilities."""
# external
from fakeredis import FakeStrictRedis as Redis
import rq
from rq import Queue

# module
from funsies import dag
from funsies import take, morph, put, shell


def test_dag_build() -> None:
    """Test simple DAG build."""
    db = Redis()
    dat = put(db, "bla bla")
    step1 = morph(db, dat, lambda x: x.decode().upper().encode())
    step2 = shell(db, "cat file1 file2", inp=dict(file1=step1, file2=dat))
    output = step2.stdout

    dag_of = dag.build_dag(db, output.hash)
    assert len(db.smembers(dag_of + ".root")) == 2

    # test deletion
    dag.delete_dag(db, output.hash)
    assert len(db.smembers(dag_of + ".root")) == 0

    # test new dag
    dag_of = dag.build_dag(db, step1.hash)
    assert len(db.smembers(dag_of + ".root")) == 1


# # process a dag
# import redis

# db = redis.Redis()
# dat = put(db, "bla bla")
# step1 = morph(db, dat, lambda x: x.decode().upper().encode())
# step2 = shell(db, "sleep 3.0; cat file1 file2", inp=dict(file1=step1, file2=dat))
# output = step2.stdout
# # q = Queue(connection=db, default_timeout=-1, is_async=False)
# q = Queue(connection=db, default_timeout=-1)

# # get dag
# d = dag.get_dag(db, output.hash)

# # We start a run for each node that has no dependencies
# for n in d.nodes:
#     if len(d.pred[n]) == 0:
#         q.enqueue_call(dag.rq_eval, args=(n, d))


# import time

# for i in range(100):
#     out = take(db, output)
#     time.sleep(0.3)
#     if out is not None:
#         print(out)
#         print(i)
#         break
