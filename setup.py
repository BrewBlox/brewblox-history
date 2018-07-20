from setuptools import find_packages, setup

setup(
    name='brewblox-history',
    use_scm_version={'local_scheme': lambda v: ''},
    long_description=open('README.md').read(),
    url='https://github.com/BrewBlox/brewblox-history',
    author='BrewPi',
    author_email='development@brewpi.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: End Users/Desktop',
        'Topic :: System :: Hardware',
    ],
    keywords='brewing brewpi brewblox embedded plugin service',
    packages=find_packages(exclude=['test']),
    install_requires=[
        'brewblox-service~=0.11.0',
        'aioinflux~=0.3',
        'dpath~=1.4',
        'aiohttp-sse~=2.0',
    ],
    python_requires='>=3.6',
    extras_require={'dev': ['tox']},
    setup_requires=['setuptools_scm'],
)
