# navira-peer

<a href="https://github.com/fusetim/navira"><img src="https://img.shields.io/badge/project-navira-yellow.svg?style=flat-square" /></a>
[![dependency status](https://deps.rs/repo/github/fusetim/navira/status.svg?path=libs%2Fnavira-peer)](https://deps.rs/repo/github/fusetim/navira?path=libs%2Fnavira-peer)
[![Crates.io](https://img.shields.io/crates/v/navira-peer.svg)](https://crates.io/crates/navira-peer)
[![docs.rs](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/navira-peer)

Minimal library to manage libp2p peer identities, keys, and eventually peer storage.

*See the [libp2p specification](https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md) for more details.*

## Supported Key Types

- [x] Ed25519 keys, required by libp2p specification
- [ ] RSA keys, optional but commonly used by legacy IPFS nodes
- [ ] Secp256k1 keys, optional
- [ ] ECDSA keys, optional

## License

This particular crate is dual-licensed under MIT and Apache-2.0 licenses.  
See the [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE) files for more details.