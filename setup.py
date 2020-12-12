"""Install funsies."""
import setuptools

# TODO: FINISH
setuptools.setup(
    name="funsies",
    version="0.1.0",
    author="Cyrille Lavigne",
    author_email="cyrille.lavigne@mail.utoronto.ca",
    description="TODO",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="src"),
    # Dependencies
    python_requries=">=3.6",
    install_requires=["diskcache"]
    # TODO
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    # url="https://github.com/pypa/sampleproject",
)
