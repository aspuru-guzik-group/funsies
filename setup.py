"""Install funsies."""
import setuptools

setuptools.setup(
    name="funsies",
    version="0.2.0",
    author="Cyrille Lavigne",
    author_email="cyrille.lavigne@mail.utoronto.ca",
    description="Funsies is an opinionated typed python library to build"
    + " reproducible, composable and data-persistent computational workflows "
    + "that are described entirely in Python.",
    url="https://github.com/aspuru-guzik-group/funsies",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="src"),
    # Dependencies
    python_requires=">=3.8",
    install_requires=[
        "redis",
        "msgpack",
        "cloudpickle",
        "rq>=1.7",
        "loguru",
    ],
    entry_points="""
        [console_scripts]
        funsies=funsies.cli:main
    """,
)
