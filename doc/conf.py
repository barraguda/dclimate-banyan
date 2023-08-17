# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "dClimate Banyan"
copyright = "2023, Chris Rossi, Rüdiger Klaehn, et al"
author = "Chris Rossi, Rüdiger Klaehn, et al"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc", "sphinx.ext.doctest", "sphinx.ext.napoleon"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]

rst_epilog = f".. |project| replace:: {project}"

doctest_global_setup = """
import dc_banyan

data_definition = dc_banyan.DataDefinition([
    ("ts", dc_banyan.Timestamp, True),
    ("one", dc_banyan.Integer, False),
    ("two", dc_banyan.Float, True),
    ("three", dc_banyan.String, False),
    ("four", dc_banyan.Enum("foo", "bar", "baz"), False)
])
"""
