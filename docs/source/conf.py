# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute.
#
import os
import sys
import json
import warnings

project = 'ruigi'
version = '1.0.0'
git_location = 'https://www.github.com/datails/ruigi'

html_theme = 'sphinx_material'
html_logo = '_static/icons/logo-contrast.svg'
html_title = project

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# You must set html_sidebars in order for the side bar to appear. There are four in the complete set.
html_sidebars = {
    "**": ["logo-text.html", "globaltoc.html", "localtoc.html", "searchbox.html"]
}

# Material theme options (see theme.conf for more information)
html_theme_options = {

    # Set the name of the project to appear in the navigation.
    'nav_title': ' '.join([project.title(), 'v'+version]),

    # Set you GA account ID to enable tracking
    'google_analytics_account': 'UA-XXXXX',

    # Specify a base_url used to generate sitemap.xml. If not
    # specified, then no sitemap will be built.
    # 'base_url': 'https://project.github.io/project',

    # Set the color and the accent color
    'color_primary': 'green',
    'color_accent': 'blue',

    # Set the repo location to get a badge with stats
    'repo_url': git_location,
    'repo_name': project,

    # Visible levels of the global TOC; -1 means unlimited
    'globaltoc_depth': 2,
    # If False, expand all TOC entries
    'globaltoc_collapse': True,
    # If True, show hidden TOC entries
    'globaltoc_includehidden': False,
}

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.intersphinx',
    "sphinxcontrib.drawio",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary'
    "sphinxcontrib.images",
    "nbsphinx",
    "recommonmark",
    "sphinx_markdown_tables"
]

autosummary_generate = True

drawio_headless = 'auto'
drawio_no_sandbox = True

images_config = {
    'override_image_directive':True
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build', '**.ipynb_checkpoints', '*/autosummary/*.rst',
                    'Thumbs.db', '.DS_Store']
