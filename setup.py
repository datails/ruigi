from setuptools import setup, find_packages

import codecs
import os
import re

here = os.path.abspath(os.path.dirname(__file__))


min_requires = [
    'luigi', "retry"
]

extras_require = {
    "dev": ['pytest', 'bumpversion', "sphinx-rtd-theme", "sphinx"],
}
extras_require["complete"] = sorted(
    {v for req in extras_require.values() for v in req}
)


def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

with open('README.md') as fobj:
    long_description = fobj.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        version_file,
        re.M,
    )
    if version_match:
        return version_match.group(1)

    raise RuntimeError("Unable to find version string.")


setup(
    name='ruigi',
    setup_requires=["wheel"],
    packages=find_packages(exclude=['docs', 'doc']),
    version=find_version("ruigi/__init__.py"),
    license='Datails',
    description='Manage your pipelines easily.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Datails',
    maintainer='Datails',
    author_email='',
    url='https://github.com/datails/ruigi',
    keywords=['luigi', 'scheduling', ],
    install_requires=min_requires,
    extras_require=extras_require,
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 3 - Alpha',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        "Operating System :: OS Independent",
    ],
)
