# navira-car

<a href="https://github.com/fusetim/navira"><img src="https://img.shields.io/badge/project-navira-yellow.svg?style=flat-square" /></a>
[![dependency status](https://deps.rs/repo/github/fusetim/navira/status.svg?path=libs%2Fnavira-car)](https://deps.rs/repo/github/fusetim/navira?path=libs%2Fnavira-car)
[![Crates.io](https://img.shields.io/crates/v/navira-car.svg)](https://crates.io/crates/navira-car)
[![docs.rs](https://img.shields.io/badge/api-rustdoc-blue.svg)](https://docs.rs/navira-car)

Utility library for working with Content Addressable aRchives (CAR files) in Rust. 
This library provides functionality to create, read, and manipulate CAR files, which are commonly used by IPFS related tools.

This library is part of the [Navira project](https://github.com/fusetim/navira), which is a modular set of applications to support and serve IPFS resources for the masses.

Unlike other similar library, such as [rs-car](https://crates.io/crates/rs-car), [rust-car](https://crates.io/crates/rust-car), or [blockless-car](https://crates.io/crates/blockless-car), `navira-car` is designed to be a sans-io library, meaning that it does not perform any I/O operations directly. Instead, it provides a set of APIs that can be used to read from and write to CAR files using any I/O mechanism (e.g., file system, network, in-memory buffers, etc). This makes it more flexible and able to support both sync and async operations and a wider range of use cases.

## Features
- [ ] Create CAR files from a set of data blocks.
- [x] Read and extract data from existing CAR files.
- [x] Support for CARv1 and CARv2 formats.
- [ ] CARv2 indexing support
  - [ ] Read CARv2 index from existing CARv2 files.
  - [ ] Create CARv2 index for new CARv2 files.
  - [ ] Reindex existing CARv2 files with new index.
  - [ ] Support for "detached" CARv2 index files (useful for IPNI).
- [x] sans-io API for easy integration into other projects.

## License

This particular crate is dual-licensed under MIT and Apache-2.0 licenses.  
See the [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE) files for more details.