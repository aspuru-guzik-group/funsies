from funsies import execute, Fun, reduce, shell
with Fun():
    # you can run shell commands
    cmd = shell('sleep 2; echo 👋 🪐')

    # and python ones
    python = reduce(sum, [3, 2])

    execute(cmd, python)
    print(f"my outputs are saved to {cmd.stdout.hash[:5]} and {python.hash[:5]}")
