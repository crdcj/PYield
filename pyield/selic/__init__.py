"""
selic — Selic-related market data and analytics.

Submodules
----------
cpm
    Raw B3 COPOM Digital Option (CPM) contract data.
probabilities (future)
    Implied COPOM meeting probabilities from CPM prices.
"""

from pyield.selic import cpm

__all__ = ["cpm"]
