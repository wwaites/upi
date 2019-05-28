from setuptools import setup

setup(
    name="upi",
    version="0.1",
    packages=["upi"],
    entry_points = {
        "console_scripts": [
            "upid = upi.upid:main",
        ]
    }
)
