"""Python wrapper of the GoodData's REST API.
See:
https://gooddata.com
https://developer.gooddata.com/api
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='grey_poupon',
    version='0.1.dev1',

    description='Python wrapper of the GoodData\'s REST API.',
    long_description=long_description,

    url='https://github.com/aviDms/GreyPoupon',

    author='Avram Dames',
    author_email='avram.dames@gmail.com',

    license='MIT',

    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='gooddata gray pages setuptools development',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['requests', 'Click'],

    # $ pip install -e .[dev,test]

    entry_points = '''
        [console_scripts]
        gp=grey_poupon:gp_cli
    ''',
)
