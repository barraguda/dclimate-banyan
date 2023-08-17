# dClimate Banyan

Software for encoding, publishing, and reading time series climate data.

## Quick Start

For development, and until a binary wheel is released, a [Rust
toolchain](https://www.rust-lang.org/tools/install) is required to build and use
this library. Install and activate a Python virtual environment using your
preferred method. Testing has been done using Python 3.9. Other versions may or
may not work at this time.

Then:

    $ pip install -U pip setuptools
    $ pip install -e py-dclimate-banyan[dev,doc]
    $ cd python-example
    $ python example.py

To build the documentation:

    $ cd doc
    $ make html

When this package sees a release, binary wheels will be available containing
compiled Rust code, so a Rust toolchain will not be needed to use released
versions of the library. Also, built documentation will probably be published
somewhere like [Read The Docs](https://about.readthedocs.com/).
