# raft-go-datastore

The package exports the `Store` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package here](https://github.com/hashicorp/raft).

This implementation uses [go-datastore](https://github.com/daotl/go-datastore) as the backend.

## License

[MIT](LICENSE) © DAOT Labs.
