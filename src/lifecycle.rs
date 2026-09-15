use std::str::FromStr;

use lenso_migration::{AppliedMigration, HistoryError, MigrationStatus};

use sqlx::{
    AssertSqlSafe, Connection, PgConnection, PgPool, Postgres, Row, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::{Migration, PostgresKitError, SchemaPlan, SetupOutcome, UpgradeOutcome};

const LEDGER_TABLE: &str = "_lenso_schema_migrations";

/// A verified, schema-scoped `PostgreSQL` pool for one Plugin.
#[derive(Clone, Debug)]
pub struct OwnedPostgres {
    plan: SchemaPlan,
    pool: PgPool,
}

impl OwnedPostgres {
    /// Connects and verifies the exact owned schema before exposing a pool.
    ///
    /// This is the runtime preparation path. It never creates a schema or
    /// applies a migration.
    pub async fn prepare(database_url: &str, plan: SchemaPlan) -> Result<Self, PostgresKitError> {
        Self::prepare_with_pool_options(database_url, plan, PgPoolOptions::new()).await
    }

    /// Connects with caller-selected pool sizing and verifies the exact schema.
    pub async fn prepare_with_pool_options(
        database_url: &str,
        plan: SchemaPlan,
        pool_options: PgPoolOptions,
    ) -> Result<Self, PostgresKitError> {
        let search_path = format!("{},pg_catalog", plan.schema());
        let connect_options = PgConnectOptions::from_str(database_url)
            .map_err(|error| PostgresKitError::database("parse connection options", error))?
            .options([
                ("search_path", search_path.as_str()),
                ("application_name", "lenso-postgres-kit"),
            ]);
        let pool = pool_options
            .connect_with(connect_options)
            .await
            .map_err(|error| PostgresKitError::database("connect runtime pool", error))?;

        if let Err(error) = verify_pool(&pool, &plan).await {
            pool.close().await;
            return Err(error);
        }
        Ok(Self { plan, pool })
    }

    /// Returns the verified Plugin-owned schema name.
    pub fn schema(&self) -> &str {
        self.plan.schema()
    }

    /// Returns the verified schema version.
    pub fn schema_version(&self) -> u64 {
        self.plan.current_version()
    }

    /// Returns the `SQLx` pool scoped to the owned schema through `search_path`.
    ///
    /// Production deployments should also use one non-superuser `PostgreSQL`
    /// role per Plugin; `PostgreSQL` grants, not `search_path`, are the security
    /// boundary against access to another Plugin's schema.
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Explicit operator-only setup and upgrade workflows for one owned schema.
#[derive(Debug)]
pub struct SchemaOperator {
    plan: SchemaPlan,
    pool: PgPool,
}

impl SchemaOperator {
    /// Connects an operator without mutating the database.
    pub async fn connect(database_url: &str, plan: SchemaPlan) -> Result<Self, PostgresKitError> {
        let connect_options = PgConnectOptions::from_str(database_url)
            .map_err(|error| PostgresKitError::database("parse connection options", error))?
            .options([("application_name", "lenso-postgres-kit-operator")]);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .map_err(|error| PostgresKitError::database("connect operator pool", error))?;
        Ok(Self { plan, pool })
    }

    /// Creates a missing schema and applies the complete authored plan atomically.
    ///
    /// Setup is idempotent for an already-current managed schema, but will not
    /// adopt an unmanaged schema or perform an upgrade.
    pub async fn setup(&self) -> Result<SetupOutcome, PostgresKitError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| PostgresKitError::database("acquire setup connection", error))?;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|error| PostgresKitError::database("begin setup transaction", error))?;
        acquire_schema_lock(&mut transaction, self.plan.schema()).await?;

        match inspect_schema(&mut transaction, &self.plan).await? {
            SchemaState::Missing => {
                create_managed_schema(&mut transaction, &self.plan).await?;
                apply_migrations(&mut transaction, &self.plan, self.plan.migrations()).await?;
                transaction
                    .commit()
                    .await
                    .map_err(|error| PostgresKitError::database("commit schema setup", error))?;
                Ok(SetupOutcome::Created {
                    version: self.plan.current_version(),
                    applied: self.plan.migrations().len(),
                })
            }
            SchemaState::Unmanaged => Err(PostgresKitError::UnmanagedSchema {
                schema: self.plan.schema().to_owned(),
            }),
            SchemaState::Managed { applied } => {
                let status = validate_history(&self.plan, &applied)?;
                let current = status.current_version;
                if status.is_current() {
                    Ok(SetupOutcome::AlreadyCurrent { version: current })
                } else {
                    Err(PostgresKitError::UpgradeRequired {
                        schema: self.plan.schema().to_owned(),
                        current,
                        expected: self.plan.current_version(),
                    })
                }
            }
        }
    }

    /// Applies only pending migrations to an existing managed schema atomically.
    pub async fn upgrade(&self) -> Result<UpgradeOutcome, PostgresKitError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| PostgresKitError::database("acquire upgrade connection", error))?;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|error| PostgresKitError::database("begin upgrade transaction", error))?;
        acquire_schema_lock(&mut transaction, self.plan.schema()).await?;

        let applied = match inspect_schema(&mut transaction, &self.plan).await? {
            SchemaState::Missing => {
                return Err(PostgresKitError::SetupRequired {
                    schema: self.plan.schema().to_owned(),
                });
            }
            SchemaState::Unmanaged => {
                return Err(PostgresKitError::UnmanagedSchema {
                    schema: self.plan.schema().to_owned(),
                });
            }
            SchemaState::Managed { applied } => applied,
        };
        let status = validate_history(&self.plan, &applied)?;
        let current = status.current_version;
        if status.is_current() {
            return Ok(UpgradeOutcome::AlreadyCurrent { version: current });
        }

        let applied_count = status.pending.len();
        apply_migrations(&mut transaction, &self.plan, status.pending).await?;
        transaction
            .commit()
            .await
            .map_err(|error| PostgresKitError::database("commit schema upgrade", error))?;
        Ok(UpgradeOutcome::Applied {
            from: current,
            to: self.plan.current_version(),
            applied: applied_count,
        })
    }
}

#[derive(Debug)]
enum SchemaState {
    Missing,
    Unmanaged,
    Managed { applied: Vec<AppliedMigration> },
}

async fn verify_pool(pool: &PgPool, plan: &SchemaPlan) -> Result<(), PostgresKitError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|error| PostgresKitError::database("acquire verification connection", error))?;
    match inspect_schema(&mut connection, plan).await? {
        SchemaState::Missing => Err(PostgresKitError::SetupRequired {
            schema: plan.schema().to_owned(),
        }),
        SchemaState::Unmanaged => Err(PostgresKitError::UnmanagedSchema {
            schema: plan.schema().to_owned(),
        }),
        SchemaState::Managed { applied } => {
            let status = validate_history(plan, &applied)?;
            let current = status.current_version;
            if status.is_current() {
                Ok(())
            } else {
                Err(PostgresKitError::UpgradeRequired {
                    schema: plan.schema().to_owned(),
                    current,
                    expected: plan.current_version(),
                })
            }
        }
    }
}

async fn inspect_schema(
    connection: &mut PgConnection,
    plan: &SchemaPlan,
) -> Result<SchemaState, PostgresKitError> {
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT roles.rolname::text\n\
         FROM pg_namespace AS namespaces\n\
         JOIN pg_roles AS roles ON roles.oid = namespaces.nspowner\n\
         WHERE namespaces.nspname = $1",
    )
    .bind(plan.schema())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|error| PostgresKitError::database("inspect schema ownership", error))?;
    let Some(owner) = owner else {
        return Ok(SchemaState::Missing);
    };

    let current_role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut *connection)
        .await
        .map_err(|error| PostgresKitError::database("inspect current database role", error))?;
    if owner != current_role {
        return Err(PostgresKitError::OwnershipMismatch {
            schema: plan.schema().to_owned(),
            owner,
            current_role,
        });
    }

    let ledger_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (\n\
           SELECT 1\n\
           FROM pg_class AS relations\n\
           JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace\n\
           WHERE namespaces.nspname = $1\n\
             AND relations.relname = $2\n\
             AND relations.relkind = 'r'\n\
         )",
    )
    .bind(plan.schema())
    .bind(LEDGER_TABLE)
    .fetch_one(&mut *connection)
    .await
    .map_err(|error| PostgresKitError::database("inspect migration ledger", error))?;
    if !ledger_exists {
        return Ok(SchemaState::Unmanaged);
    }

    let ledger = qualified_table(plan.schema(), LEDGER_TABLE);
    let read_ledger = format!("SELECT version, name, checksum FROM {ledger} ORDER BY version");
    let rows = sqlx::query(AssertSqlSafe(read_ledger))
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| PostgresKitError::database("read migration ledger", error))?;
    let mut applied = Vec::with_capacity(rows.len());
    for row in rows {
        let version: i64 = row
            .try_get("version")
            .map_err(|error| PostgresKitError::database("decode migration version", error))?;
        let version = u64::try_from(version).map_err(|_| PostgresKitError::HistoryDiverged {
            schema: plan.schema().to_owned(),
            version: 0,
        })?;
        applied.push(AppliedMigration {
            version,
            name: row
                .try_get("name")
                .map_err(|error| PostgresKitError::database("decode migration name", error))?,
            checksum: row
                .try_get("checksum")
                .map_err(|error| PostgresKitError::database("decode migration checksum", error))?,
        });
    }
    Ok(SchemaState::Managed { applied })
}

fn validate_history(
    plan: &SchemaPlan,
    applied: &[AppliedMigration],
) -> Result<MigrationStatus, PostgresKitError> {
    plan.status(applied).map_err(|error| match error {
        HistoryError::HistoryDiverged { version } => PostgresKitError::HistoryDiverged {
            schema: plan.schema().to_owned(),
            version,
        },
        HistoryError::SchemaAhead { actual, expected } => PostgresKitError::SchemaAhead {
            schema: plan.schema().to_owned(),
            actual,
            expected,
        },
    })
}

async fn acquire_schema_lock(
    transaction: &mut Transaction<'_, Postgres>,
    schema: &str,
) -> Result<(), PostgresKitError> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(\n\
           hashtextextended(current_database() || ':' || $1, 0)\n\
         )",
    )
    .bind(schema)
    .execute(&mut **transaction)
    .await
    .map_err(|error| PostgresKitError::database("lock owned schema", error))?;
    Ok(())
}

async fn create_managed_schema(
    transaction: &mut Transaction<'_, Postgres>,
    plan: &SchemaPlan,
) -> Result<(), PostgresKitError> {
    let schema = quote_identifier(plan.schema());
    sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut **transaction)
        .await
        .map_err(|error| PostgresKitError::database("create owned schema", error))?;
    let ledger = qualified_table(plan.schema(), LEDGER_TABLE);
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {ledger} (\n\
           version bigint PRIMARY KEY CHECK (version > 0),\n\
           name text NOT NULL,\n\
           checksum text NOT NULL,\n\
           applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()\n\
         )"
    )))
    .execute(&mut **transaction)
    .await
    .map_err(|error| PostgresKitError::database("create migration ledger", error))?;
    Ok(())
}

async fn apply_migrations(
    transaction: &mut Transaction<'_, Postgres>,
    plan: &SchemaPlan,
    pending: &[Migration],
) -> Result<(), PostgresKitError> {
    let search_path = format!(
        "SET LOCAL search_path TO {}, pg_catalog",
        quote_identifier(plan.schema())
    );
    sqlx::raw_sql(AssertSqlSafe(search_path))
        .execute(&mut **transaction)
        .await
        .map_err(|error| PostgresKitError::database("select owned schema", error))?;

    let ledger = qualified_table(plan.schema(), LEDGER_TABLE);
    for migration in pending {
        sqlx::raw_sql(migration.sql())
            .execute(&mut **transaction)
            .await
            .map_err(|error| PostgresKitError::database("apply owned migration", error))?;
        record_migration(transaction, &ledger, migration).await?;
    }
    Ok(())
}

async fn record_migration(
    transaction: &mut Transaction<'_, Postgres>,
    ledger: &str,
    migration: &Migration,
) -> Result<(), PostgresKitError> {
    let version = i64::try_from(migration.version()).expect("validated migration version fits i64");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {ledger} (version, name, checksum) VALUES ($1, $2, $3)"
    )))
    .bind(version)
    .bind(migration.name())
    .bind(migration.checksum())
    .execute(&mut **transaction)
    .await
    .map_err(|error| PostgresKitError::database("record owned migration", error))?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{identifier}\"")
}

fn qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}
