# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import inspect

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

import sphinx_pdj_theme
from sphinx.application import Sphinx

sys.path.insert(0, os.path.abspath("../.."))
sys.path.insert(0, os.path.abspath("../../dataverse"))


# -- Project information -----------------------------------------------------

project = "dataverse"
copyright = "2024, Upstage AI"
author = "Upstage AI"

# The full version, including alpha/beta/rc tags
release = "1.0.0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
]
todo_include_todos = True
napoleon_google_docstring = True
napoleon_numpy_docstring = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_permalinks_icon = "<span>#</span>"
html_theme = "sphinx_pdj_theme"
html_theme_path = [sphinx_pdj_theme.get_html_theme_path()]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


# -- Handle register_etl decorator -------------------------------------------------


def process_signature(
    app: Sphinx, what: str, name: str, obj, options, signature, return_annotation
):
    if what == "function" and hasattr(obj, "run"):
        original_func = obj.run.__wrapped__
        new_signature = inspect.signature(original_func)
        parameters = list(new_signature.parameters.values())
        new_signature = new_signature.replace(
            parameters=[inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
            + parameters
        )
        return str(new_signature), return_annotation

    return signature, return_annotation


def skip_undoc_members(app, what, name, obj, skip, options):
    if inspect.isclass(obj) and not hasattr(obj, "__is_etl__"):
        return None
    return True


def setup(app):
    app.connect("autodoc-process-signature", process_signature)
    app.connect("autodoc-skip-member", skip_undoc_members)
