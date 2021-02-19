"""Hello, world!"""
import funsies as fun

with fun.Fun():
    cmd = fun.shell('sleep 2; echo "👋 🪐"')
    fun.execute(cmd)
    print(f"my output is {cmd.stdout.hash}")
