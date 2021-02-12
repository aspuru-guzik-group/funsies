"""Test of Funsies shell capabilities."""
# std
import cProfile


# module
import funsies as f

with f.ManagedFun() as db:

    def build() -> f.Artefact:
        """Build the DAG."""
        outputs = []
        for i in range(1000):
            dat = f.put(f"bla{i}")
            step1 = f.morph(lambda x: x.decode().upper().encode(), dat)
            step2 = f.shell(
                "cat file1 file2",
                inp=dict(file1=step1, file2="something"),
                out=["file2"],
            )
            outputs += [f.utils.concat(step1, step1, step2.stdout, join=" ")]

        final = f.utils.concat(*outputs, join="\n")
        return final

    def build_dag(final: f.Artefact) -> None:
        """Build internal dag repr."""
        f.dag.build_dag(db, final.hash)

    # cProfile.run("build()", sort="tottime")
    # now:
    # 6046195 function calls (5936152 primitive calls) in 6.680 seconds

    # before pipeline artefacts:
    # 3906875 function calls in 7.565 seconds

    # before pipeline make_op
    # 6758352 function calls (6648309 primitive calls) in 9.784 seconds

    final = build()
    cProfile.run("build_dag(final)", sort="tottime")

    # now
    # 3316697 function calls in 4.564 seconds

    # before pipelining build_dag
    # 3906875 function calls in 7.933 seconds
