from setuptools import setup, find_packages

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
    name="datatotable",
    version="0.3",
    description="Create SQLite database tables automatically from data",
    license="MIT",
    classifiers=['License :: OSI Approved :: MIT License'],
    long_description=long_description,
    author="Spencer Weston",
    author_email="Spencerweston3214@gmail.com",
    url="https://github.com/Spencer-Weston/DatatoTable",
    keywords="data SQL sql SQLalchemy web-scraping data-management",
    packages=find_packages(include=['datatotable', 'datatotable.*']),
    install_requires=[
                      'SQLAlchemy>=1.2.17',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)
