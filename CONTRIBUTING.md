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

## Commit messages

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) as described in the
[organization contributing guide](https://github.com/corfedo/.github/blob/main/CONTRIBUTING.md#commit-messages), for
example `fix(sqlserver): report cancelled commands as OperationCanceledException`. Release notes are generated from
them.

## Development

Requires the .NET SDK pinned in [`global.json`](global.json) and Docker for the PostgreSQL and SQL Server tests, which
start one `postgres:18.6` or SQL Server container per test process through Testcontainers and create a database per
test class. The SQL Server tests use SQL Server 2022; `CORFEDO_WORK_SQLSERVER_IMAGE` chooses another image, such as
`mcr.microsoft.com/mssql/server:2025-latest`. `CORFEDO_WORK_SQLSERVER`, a connection string to `master`, makes the test
processes share a running server instead; `dev/check` starts one for its run, since SQL Server sizes its memory to the
host.

```sh
dev/check    # Release build, all tests, Native AOT smoke publish; fails on any warning
```

Testcontainers finds Docker through the usual environment variables. With colima on macOS:

```sh
export DOCKER_HOST=unix://<colima profile directory>/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

SQL Server images are published for `linux/amd64` only. On an Apple silicon Mac they run under Rosetta in a colima
profile of their own, about three times slower than natively, so full suites belong on an x86_64 host:

```sh
colima start sqlserver --arch aarch64 --vm-type vz --vz-rosetta --cpu 4 --memory 6 --disk 30
docker context use colima        # colima start made the new profile the current context; switch back
export DOCKER_HOST=unix://$HOME/.colima/sqlserver/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

On a Linux host with Docker running locally no variables are needed; a user outside the `docker` group needs
`DOCKER_HOST` pointing at a socket it can use. Large test runs open many short connections through Docker's port
proxy; hosts that exhaust ephemeral ports need `net.ipv4.tcp_tw_reuse = 1`.

## Design first

Behaviour is specified in [`docs/`](docs/README.md) before it is implemented. A change to the semantics of leases,
reconciliation or jobs updates the design and the [scenarios](docs/design/scenarios.md) in the same change, and every
store provider passes the conformance suite derived from those scenarios.
