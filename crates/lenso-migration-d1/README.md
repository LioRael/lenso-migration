# lenso-migration-d1

Explicit migration operations for one Plugin-owned, dedicated primary D1 database.
The portable `lenso-migration` crate owns definitions, checksums and history
comparison. This adapter owns D1 batch execution and its private ledger. Plugins
continue to own their SQL, data transformations and deployment authorization.

## Interface

- `Plan::verify`: read-only runtime preparation; absent, old, newer or changed
  histories fail closed. It never initializes or upgrades a database.
- `Plan::setup`: initialize a fresh dedicated database; reject unmanaged tables
  and other owners' histories, with a second check inside the write batch.
- `Plan::upgrade`: apply an authored pending suffix to an exact matching history.
- `Plan::adopt_legacy`: explicitly register a known v1 fingerprint without replaying
  its SQL. This is an operator decision, never a runtime fallback. It validates the
  migration record, not arbitrary physical-schema drift or data equivalence.

The physical database is selected by the injected transport, outside the plan.
The ledger keys include the Plugin owner, backend and migration version. A binding
alias is not a database identity or authorization mechanism: the deployment owner
must supply the exact target with migration authority. Do not expose these methods
on an ordinary application endpoint.

Each `SqlMigration` carries the complete immutable SQL and generated statement-end
byte offsets. Generate offsets with a SQLite-aware authoring tool (Auth uses
Python `sqlite3.complete_statement`), including quoted semicolons and triggers.
The adapter validates exact coverage and UTF-8 boundaries; it does not implement
a SQL parser. Statements remain authored trusted SQL. Migration SQL must not
modify the adapter's `_lenso_migrations` or `_lenso_migration_guard` tables.

`Transport::batch` must use the primary D1 binding and return each statement's
rows in order, rejecting any unsuccessful result. A pending suffix and its
history/legacy guards are sent in **one atomic D1 batch**. This adapter imposes
128 statements and 100 bound parameters per statement; larger plans fail before
submission. Split an oversized deployment into explicitly reviewed releases, not
silent partial transactions.

The [D1 batch contract](https://developers.cloudflare.com/d1/worker-api/d1-database/#batch)
supplies ordered execution and rollback on failure. A lost response is different:
the write may have committed. The adapter never retries it. Inspect the primary
history before deciding the next deployment action.

## Validation

The lifecycle suite uses real SQLite transactions for setup/upgrade, drift, stale
plans, unmanaged-database races, explicit legacy adoption and unknown write
outcomes. Auth's `experiments/workers-g4/migration-proof.mjs` additionally runs
the actual Rust adapter through local workerd primary D1 bindings for seven owners.

No production migration, registry publication or cross-database data transfer is
performed by this library.
