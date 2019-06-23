from setuptools import setup, find_packages

# setup metainfo
libinfo_content = open('__init__.py', 'r').readlines()
version_line = [l.strip() for l in libinfo_content if l.startswith('__version__')][0]
exec(version_line)  # produce __version__

setup(
    name='zerotasq',
    version=__version__,
    description='Stream tasks to multiple worker processes using ZeroMQ.',
    url='https://github.com/chschock/zerotasq',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    author='Christoph Schock',
    author_email='chschock@gmail.com',
    license='MIT',
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        'pyzmq>=17.1.0',
        'termcolor>=1.1'
    ],
    classifiers=(
        'Programming Language :: Python :: 3.6',
        'License :: MIT License',
        'Operating System :: Linux',
        'Topic :: Scientific/Engineering',
    ),
    keywords='multiprocessing stream processing zeromq load balancer',
)
