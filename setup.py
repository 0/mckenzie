import setuptools

setuptools.setup(
    name='mckenzie',
    version='0.2.0+dev',
    author='Dmitri Iouchtchenko',
    author_email='diouchtc@uwaterloo.ca',
    url='https://github.com/0/mckenzie',
    packages=['mckenzie'],
    scripts=['bin/mck'],
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
    ],
    install_requires=[
        # For pkg_resources.
        'setuptools',
        # Cannot depend on psycopg2, in order to support users of psycopg2-binary.
        #'psycopg2',
    ],
    # For logging stacklevel.
    python_requires='>=3.8',
)
