"""Install funsies."""
import setuptools
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="funsies",
    version="0.8.0",
    author="Cyrille Lavigne",
    author_email="cyrille.lavigne@mail.utoronto.ca",
    description="Funsies is a library to build and execution engine for"
    + " reproducible, composable and data-persistent computational workflows.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aspuru-guzik-group/funsies",
    package_dir={"": "src"},
    package_data={"funsies": ["py.typed"]},  # mypy exports
    packages=setuptools.find_namespace_packages(where="src"),
    # Dependencies
    python_requires=">=3.7",
    install_requires=[
        "mypy_extensions",
        "redis",
        "cloudpickle",
        "rq>=1.7",
        "loguru",
        'importlib-metadata ~= 1.0 ; python_version < "3.8"',
        'typing_extensions ; python_version < "3.8"',
        "chevron",
    ],
    entry_points="""
        [console_scripts]
        funsies=funsies._cli:main
        start-funsies=funsies._start_funsies:main
    """,
    classifiers=[
        "Development Status :: 4 - Beta",
        #
        "Typing :: Typed",
        #
        "License :: OSI Approved :: MIT License",
        #
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        #
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Chemistry",
        "Topic :: Scientific/Engineering :: Physics",
    ],
    keywords="workflows hashtree redis compchem chemistry parallel hpc",
)
