from setuptools import setup

setup(
    name="upi",
    version="0.1",
    packages=["upi"],
    install_requires=["mpi4py"],
    entry_points = {
        "console_scripts": [
            "upid = upi.upid:main",
        ]
    }
)
