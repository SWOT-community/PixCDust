# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'pixcdust'
copyright = '2025, Zawadzki Lionel'
author = 'Zawadzki Lionel'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.viewcode','autoapi.extension','nbsphinx',"sphinxcontrib.collections"]

html_show_sourcelink = False
set_type_checking_flag = True
nbsphinx_allow_errors = True

templates_path = ['_templates']
exclude_patterns = []
autoapi_dirs = ['../pixcdust']
source_suffix = ['.rst']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']


# manual build path
collections = {
    'notebooks': {
        'driver': 'copy_folder',
        'source': 'pixcdust/notebooks',
        'target': 'notebooks/',
        'ignore': ['*.py', '.sh'],
        'safe': False,
    }
}

# ReadTheDoc build path

collections = {
    'notebooks': {
        'driver': 'copy_folder',
        'source': '../pixcdust/notebooks',
        'target': 'notebooks/',
        'ignore': ['*.py', '.sh'],
        'safe': False,
    }
}
