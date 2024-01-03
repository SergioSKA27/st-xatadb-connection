from setuptools import setup,find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def get_version(rel_path):
    with open(rel_path, "r", encoding="UTF-8") as f:
        for line in f:
            if line.startswith("__version__"):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")

setup(
    name = "st-xatadb-connection",
    version = get_version("src/st_xatadb_connection/__init__.py"),
    author = "Sergio Demis Lopez Martinez",
    author_email = "sergioska81@hotmail.com",
    description = ("Streamlit Xata Data Base Connection"
                                   "An easy way to connect your Streamlit application to your Xata database."),
    license = "MIT",
    keywords=["streamlit", "xata", "connection", "integration", "database"],
    url = "https://github.com/SergioSKA27/st-xatadb-connection",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Environment :: Plugins",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: User Interfaces",
    ],
    install_requires=['streamlit>=1.28','xata'],
)

