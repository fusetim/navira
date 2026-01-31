# navira-store

Navira Store is a service that provides access to locally stored IPFS content.  
It is designed to be simple, efficient, and robust, making it easy to serve IPFS content to end users.

## How does it work?

Navira Store is a very simple and dump server : it serves static IPFS content from a local storage. 
All content must be pre-stored in the local storage, as CAR archives. Navira Store does not fetch or pin any content from the IPFS network.

On startup, Navira Store scans its local storage for CAR files, and builds an index of the available content.
It then listens for incoming `/ipfs/bitswap` (v1.2) requests, and serves the requested content if it is available in the local storage.

Navira Store will adhere to the IPFS Bitswap v1.2 protocol, and will respond to requests for blocks and CIDs.
However, this is not a libp2p nor an IPFS node, it will serve this content unencrypted over a plaintext socket (Unix or UDP).
This makes it very easy to deploy and use, but also means that it is not suitable for all use cases.

