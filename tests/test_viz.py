"""Test of visualization routines."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import dag, execute, Fun, morph, options, put, shell, take, utils
from funsies.constants import DAG_INDEX, DAG_STORE, hash_t
from funsies.utils import concat


def test_dag_build() -> None:
    """Test simple DAG build."""
    with Fun(Redis()) as db:
        dat = put("bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        step3 = shell("cat file1", inp=dict(file1=step2.stdout))
        out = utils.concat(step1, dat, step2.stdout, step3.stdout)
        dag.build_dag(db, out.hash)
        out = dag.dot(db, out.hash)
        print(out)


test_dag_build()
