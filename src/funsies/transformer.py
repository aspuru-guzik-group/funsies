"""A very simple IO transformer."""

transformer_code = """
import base64
import sys

import cloudpickle

with open(sys.argv[1], "rb") as f:
    data = base64.decodebytes(f.read())

tform = cloudpickle.loads(data)
with open("__metadata__.pkl", "rb") as f:
    meta = cloudpickle.load(f)

inputs = {}
outputs = {}
for fil in meta["inputs"]:
    inputs[fil] = open(fil, "r")

for fil in meta["outputs"]:
    try:
        outputs[fil] = open(fil, "r+")
    except FileNotFoundError:
        outputs[fil] = open(fil, "w")

tform(inputs, outputs)
""".encode()
