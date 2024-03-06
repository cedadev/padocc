__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

# Job Subset Allocation Script
# - Calculate Expected Utilisation for each Job (500 + Kerchunk Size*5 MB)

# First-Fit Bin Packing Algorithm
# - Sort Utilisations from largest to smallest
# - Bin Capacity is largest size rounded up to next memory cap (1, 2, 3, 4 GB)
# - Allocate item to first bin with space remaining
# - End with N bins (subsets) - write list of project codes for each subset to a separate file in proj_code_subsets/set_N.txt
# - Run array with number of subsets already set.

# Utilisation estimate is (total_chunks * 835) + 500 (MB)
"""
for proj_code in (repeat_id set):
    open detail-cfg (for this code)
    calculate utilisation
    add to dict [utilisation, proj_code]
    keep track of max/min
get bins using binpacking (pypi)
"""