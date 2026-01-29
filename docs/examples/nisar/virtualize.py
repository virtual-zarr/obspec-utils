#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "virtualizarr[hdf] @ git+https://github.com/zarr-developers/VirtualiZarr.git@main",
#     "obspec-utils @ git+https://github.com/virtual-zarr/obspec-utils@main",
#     "aiohttp",
# ]
# ///
"""
Minimal example: Create a virtual datatree from a NISAR file.

This script demonstrates how to use VirtualiZarr with obspec-utils to
create a virtual representation of a remote HDF5 file without downloading it.

KNOWN LIMITATIONS (will fail):
    1. The "crosstalk" variable has a complex dtype not supported by Zarr.
       We use `drop_variables=["crosstalk"]` to skip it, but...
    2. `drop_variables` doesn't yet work for variables in nested HDF5 groups.
       The crosstalk variable is in a nested group, so it still gets parsed.

These issues require upstream fixes in VirtualiZarr. See:
    https://github.com/zarr-developers/VirtualiZarr

Usage:
    uv run --script docs/examples/nisar/virtualize.py
"""

from urllib.parse import urlparse

import earthaccess
import virtualizarr as vz

from obspec_utils.stores import AiohttpStore
from obspec_utils.registry import ObjectStoreRegistry


def main():
    # Authenticate with NASA Earthdata
    print("Authenticating with NASA Earthdata...")
    earthaccess.login()

    # Query for NISAR data
    query = earthaccess.DataGranules()
    query.short_name("NISAR_L2_GCOV_BETA_V1")
    query.params["attribute[]"] = "int,FRAME_NUMBER,77"
    query.params["attribute[]"] = "int,TRACK_NUMBER,5"
    results = query.get_all()
    print(f"Found {len(results)} granules")

    # Get the HTTPS URL
    https_links = earthaccess.results.DataGranule.data_links(
        results[0], access="external"
    )
    https_url = https_links[0]
    print(f"URL: {https_url}")

    # Parse URL and get auth token
    parsed = urlparse(https_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    token = earthaccess.get_edl_token()["access_token"]

    # Create store with authentication
    store = AiohttpStore(
        base_url,
        headers={"Authorization": f"Bearer {token}"},
    )
    registry = ObjectStoreRegistry({base_url: store})

    # Create virtual datatree
    # NOTE: drop_variables is intended to skip "crosstalk" (complex dtype),
    # but it doesn't work for nested groups yet. This will currently fail.
    print("\nCreating virtual datatree...")
    parser = vz.parsers.HDFParser(drop_variables=["crosstalk"])
    vdt = vz.open_virtual_datatree(
        https_url,
        registry=registry,
        parser=parser,
        loadable_variables=[],
    )

    # Print structure
    print("\nVirtual DataTree structure:")
    print(vdt)

    # Print some stats
    print("\nVariables by group:")
    for path, node in vdt.subtree:
        ds = node.to_dataset()
        if ds.data_vars:
            print(
                f"  {path}: {list(ds.data_vars)[:5]}{'...' if len(ds.data_vars) > 5 else ''}"
            )


if __name__ == "__main__":
    main()
