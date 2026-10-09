"""HFIG Data Vault accelerator -- SDP-native, metadata-driven.

VERSION is stamped onto every generated table as a property (hfig.accelerator.version),
so a deployed table records which generator built it. That matters across 8 workspaces:
when two lakes disagree, the first question is whether they were built by the same code.
"""

VERSION = "0.2.0"
__version__ = VERSION
