#!/usr/bin/env python
# -*- coding: utf-8 -*-


from setuptools import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='lcubo_helpers',
    version='0.1.1',
    description="some functions and classes I copy paste in several projects. this is a kitten saver",
    long_description=readme + '\n\n' + history,
    author="Leonardo Lazzaro",
    author_email='llazzaro@dc.uba.ar',
    url='https://github.com/llazzaro/lcubo_helpers',
    packages=[
        'lcubo_helpers',
    ],
    package_dir={'lcubo_helpers':
                 'lcubo_helpers'},
    entry_points={
        'console_scripts': [
            'lcubo_helpers=lcubo_helpers.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='lcubo_helpers',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
