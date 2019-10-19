from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
    name="DataToTable",
    version="0.2",
    description="Create SQLite database tables automatically from data",
    license="MIT",
    long_description=long_description,
    author="Spencer Weston",
    author_email="Spencerweston3214@gmail.com",
    url="https://github.com/Spencer-Weston/DatatoTable",
    packages=['datatotable'],
    install_requires=[],  # What all do we need here?
)
