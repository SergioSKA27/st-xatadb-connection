import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "st_xata_connection",
    version = "1.0.0",
    author = "Sergio Demis Lopez Martinez",
    author_email = "sergioska81@hotmail.com",
    description = ("Streamlit Xata Data Base Connection"
                                   "An easy way to connect your Streamlit application to your Xata database."),
    license = "MIT",
    keywords = "streamlit connection xata database",
    url = "http://packages.python.org/an_example_pypi_project",
    packages=find_packages(where='src'),
    long_description=read('README'),
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
    install_requires=['streamlit>=1.28','pandas','xata']
)
