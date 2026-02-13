#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "earthaccess",
#     "virtualizarr[hdf] @ git+https://github.com/maxrjones/VirtualiZarr@c-dtype",
#     "obspec-utils @ git+https://github.com/virtual-zarr/obspec-utils@main",
#     "aiohttp",
# ]
# ///
"""
Minimal example: Create a virtual datatree from a NISAR file.

This script demonstrates how to use VirtualiZarr with obspec-utils to
create a virtual representation of a remote HDF5 file without downloading it.

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

    print("\nCreating virtual datatree...")
    parser = vz.parsers.HDFParser()
    parser(https_url, registry=registry)


if __name__ == "__main__":
    main()
