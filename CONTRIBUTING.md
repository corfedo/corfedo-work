# Contributing

Corfedo Work is licensed under [Apache-2.0](LICENSE). Contributions are accepted under the same license.

## Developer Certificate of Origin

Every commit carries a `Signed-off-by` line certifying the
[Developer Certificate of Origin 1.1](https://developercertificate.org/):

```
Signed-off-by: Your Name <you@example.com>
```

`git commit -s` adds it. Contributors retain their copyright. Contributions are licensed under Apache-2.0; no
copyright assignment or additional rights through a contributor license agreement are requested.

## Development

Requires the .NET SDK pinned in [`global.json`](global.json) and Docker for the PostgreSQL tests, which start one
`postgres:18.6` container per test process through Testcontainers and create a database per test class.

```sh
dev/check    # Release build, all tests, Native AOT smoke publish; fails on any warning
```

Testcontainers finds Docker through the usual environment variables. With colima on macOS:

```sh
export DOCKER_HOST=unix://<colima profile directory>/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

On a Linux host with Docker running locally no variables are needed; a user outside the `docker` group needs
`DOCKER_HOST` pointing at a socket it can use. Large test runs open many short connections through Docker's port
proxy; hosts that exhaust ephemeral ports need `net.ipv4.tcp_tw_reuse = 1`.

## Design first

Behaviour is specified in [`docs/`](docs/README.md) before it is implemented. A change to the semantics of leases,
reconciliation or jobs updates the design and the [scenarios](docs/design/scenarios.md) in the same change, and every
store provider passes the conformance suite derived from those scenarios.
