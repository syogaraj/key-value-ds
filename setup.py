import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='key_value_ds',
    version='1.0.2',
    author='Yogaraj.S',
    author_email='yogarajsivaprakasam@gmail.com',
    description='A key value datastore which stores data locally in a memory-mapped file.',
    license='MIT',
    url='https://github.com/syogaraj/key-value-ds',
    download_url='https://github.com/syogaraj/key-value-ds/releases/tag/v1.0.2',
    packages=['key_value_ds', 'tests'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
    extras_require={
        'dev': ['pytest', 'wheel']
    },
)