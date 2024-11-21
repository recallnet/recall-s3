# Basin S3

[![License](https://img.shields.io/github/license/tablelandnetwork/s3-ipc.svg)](./LICENSE)
[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> An S3 adapter for Basin

# Table of Contents

- [Basin S3](#basin-s3)
    - [Table of Contents](#table-of-contents)
    - [Background](#background)
    - [Usage](#usage)
    - [Development](#development)
    - [Contributing](#contributing)
    - [License](#license)

# Background

**Basin S3** is a S3 adapter for Basin. It provides a S3 compatible API that translates S3 requests into Basin transactions or requests. With that we're able to leverage any S3 client or tooling to make use of the Basin features.

The Basin S3 implementation is built on top of the [s3s](https://github.com/Nugine/s3s) project. `s3s` provides a [trait](https://github.com/Nugine/s3s/blob/main/crates/s3s/src/s3_trait.rs#L10), generated from [aws-sdk-rust](https://github.com/awslabs/aws-sdk-rust) repository. By providing an implementation of that trait we can provide an HTTP service that "speaks S3".

# Usage

You can start the server by running

```bash
cargo run --features binary -- --private-key [PRIVATE_KEY] --access-key AKEXAMPLES3S --secret-key SKEXAMPLES3S
```
In read-only mode, just omit the private key.

You can point the adapter to a specific Basin network by providing the `--network` flag. By default, it points to `ignition`. For development, you can use `localnet` or `devnet`.  

# Development

Right now, for lack of a better option, the tests rely on a running 3-node `localnet` network. So, make sure you have one running locally to run them.

# Deployment

Building the Docker image:

```bash
docker build -t basin-s3 .
```

Running it

```bash
docker run -dt \
  -e HOST=0.0.0.0 \
  -e ACCESS_KEY=S3EXAMPLEAK \
  -e SECRET_KEY=S3EXAMPLESK  \
  -e PRIVATE_KEY=PRIVATE_KEY \
  --name basin-s3 \
  --network host \
  basin-s3
```

note: `--network=host` does not work on MacOS.

# Contributing

PRs accepted.

Small note: If editing the README, please conform to the
[standard-readme](https://github.com/RichardLitt/standard-readme) specification.

# License

MIT AND Apache-2.0, © 2021-2024 Basin Network Contributors
