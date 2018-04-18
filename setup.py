from setuptools import setup, find_packages


VERSION = "0.1a2"

setup(
    name="mongodb_formatter",
    version=VERSION,
    author="Joe Drumgoole",
    author_email="joe@joedrumgoole.com",
    description="Formatting for MongoDB cursors",
    long_description="Formatting for MongoDB Cursors",
    include_package_data=True,
    keywords="MongoDB",
    url="https://github.com/jdrumgoole/mongodb_formatter",

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6'],

    install_requires=["pymongo", "nose"],
    package_data={
        '': ['*.txt', '*.rst', '*.md'],
    },

    packages=find_packages(),
    test_suite='nose.collector',
    tests_require=['nose'],
)
