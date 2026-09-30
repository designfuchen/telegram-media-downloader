from setuptools import setup

from utils import __version__

setup(
    name="telegram-media-downloader",
    version=__version__,
    author="tangyoha",
    author_email="tangyoha@outlook.com",
    description="A simple script to download media from telegram",
    py_modules=["media_downloader"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet",
        "Topic :: Communications",
        "Topic :: Communications :: Chat",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    project_urls={
    },
    python_requires=">=3.11,<3.12",
)
