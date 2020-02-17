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


setup(
    name="places",
    python_requires=">=3.7",
    version=get_version("places"),
    url="https://github.com/alexkeating/places",
    project_urls={
        "Documentation": "https://github.com/alexkeating/places",
        "Source": "https://github.com/alexkeating/places",
    },
    license="MIT",
    description="A framewwork for processing jobs.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Alex Keating",
    package_data={"places": ["py.typed"]},
    packages=get_packages("places"),
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
