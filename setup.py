import re
from pathlib import Path

from setuptools import setup


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    version = Path(package, "__version__.py").read_text()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", version).group(1)


def get_long_description():
    """
    Return the README.
    """
    long_description = ""
    with open("README.md", encoding="utf8") as f:
        long_description += f.read()
    long_description += "\n\n"
    with open("CHANGELOG.md", encoding="utf8") as f:
        long_description += f.read()
    return long_description


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [str(path.parent) for path in Path(package).glob("**/__init__.py")]


setup(
    name="async-pipelines",
    python_requires=">=3.7",
    version=get_version("pipelines"),
    url="https://github.com/alexkeating/async-pipelines",
    project_urls={
        "Documentation": "https://github.com/alexkeating/places",
        "Source": "https://github.com/alexkeating/async-pipelines",
    },
    license="MIT",
    description="A framewwork for processing jobs.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Alex Keating",
    package_data={"pipelines": ["py.typed"]},
    packages=get_packages("pipelines"),
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Framework :: AsyncIO",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
