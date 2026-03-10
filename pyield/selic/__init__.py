"""
selic — Selic-related market data and analytics.

Submodules
----------
cpm
    Raw B3 COPOM Digital Option (CPM) contract data.
probabilities
    Implied COPOM meeting probabilities from CPM prices.
"""

from pyield.selic import cpm, probabilities

__all__ = ["cpm", "probabilities"]
