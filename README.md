# Navira

Navira expects to be a modular set of applications to support and serve IPFS resources for the masses.
Its core goals are to provide a very simple, efficient, and robust way to serve IPFS content to end users
by supporting the IPFS nodes and the regular HTTP clients (through trustless gateways, delegated routers, etc).

## Current Status

Navira is currently in early development. The core libraries and applications are being built out, but
there is still a lot of work to be done. Contributions are welcome, but it might be too early. Please
create issues for any features or bugs you encounter (and only then contribute code).

## Building blocks 

Navira is composed of several building blocks (crates) that can be used independently or together:
- [`navira-store`](./apps/navira-store/): Main service that provides access to locally stored (and static currently) IPFS content.
- `navira-gateway`: HTTP trustless-gateway for IPFS content. ***TBD***
- `navira-router`: HTTP delegated-router for IPFS content. ***TBD***
- `navira-index`: IPNI Index Provider, that enable fast-lookup of IPFS content served by navira-store nodes. ***TBD***

Additionally, navira comes with libraries that can be used by the above applications or by other projects:
- [`navira-car`](./libs/navira-car/): Utility library for working with Content Addressable aRchives (CAR files) in Rust.

## License

Navira is dual-licensed under the [CeCILL v2.1](./CeCILL-LICENSE) and the [EUPL v1.2](./EUPL-LICENSE) licenses.
By using or contributing to Navira, you agree to be bound by the terms of these licenses. 

> [!NOTE]
> Some projects within Navira may have their own licenses. Especially, the libraries that are meant to be used independently,
> might be licensed under more permissive licenses (like MIT or Apache-2.0).  
> Please check the individual project documentation for more details.