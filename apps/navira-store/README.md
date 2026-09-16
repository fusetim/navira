# navira-store

Navira Store is a service that provides access to locally stored IPFS content.  
It is designed to be simple, efficient, and robust, making it easy to serve IPFS content to end users.

## How does it work?

Navira Store is a very simple and dump server : it serves static IPFS content from a local storage. 
All content must be pre-stored in the local storage, as CAR archives. Navira Store does not fetch or pin any content from the IPFS network.

On startup, Navira Store scans its local storage for CAR files, and builds an index of the available content.
Then, it exposes a simple [Trustless HTTP Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/#accept-request-header)
that allows users to retrieve the content by its CID. *Goal is to provide raw blocks 
only, and maybe extend it to support CAR retrieval later.*
At the same time, it exposes stored blocks through the [IPNI Index Provider API](https://github.com/ipni/specs/blob/main/IPNI_HTTP_PROVIDER.md).