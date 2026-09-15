use std::sync::atomic::{AtomicU64, Ordering};

use lenso_postgres_kit::{
    Migration, OwnedPostgres, PostgresKitError, SchemaOperator, SchemaPlan, SetupOutcome,
    UpgradeOutcome,
};
use sqlx::{AssertSqlSafe, Executor, PgPool, postgres::PgPoolOptions};

static NEXT_SCHEMA: AtomicU64 = AtomicU64::new(0);

const V1: &[Migration] = &[Migration::new(
    1,
    "create-counters",
    "CREATE TABLE counters (name text PRIMARY KEY, value bigint NOT NULL)",
)];
const V2: &[Migration] = &[
    Migration::new(
        1,
        "create-counters",
        "CREATE TABLE counters (name text PRIMARY KEY, value bigint NOT NULL)",
    ),
    Migration::new(
        2,
        "add-updated-at",
        "ALTER TABLE counters ADD COLUMN updated_at timestamptz;\n\
         UPDATE counters SET updated_at = transaction_timestamp();\n\
         ALTER TABLE counters ALTER COLUMN updated_at SET NOT NULL",
    ),
];
const DRIFTED_V1: &[Migration] = &[Migration::new(
    1,
    "create-counters",
    "CREATE TABLE counters (name text PRIMARY KEY, value integer NOT NULL)",
)];
const BROKEN_SETUP: &[Migration] = &[
    Migration::new(
        1,
        "create-markers",
        "CREATE TABLE markers (id bigint PRIMARY KEY)",
    ),
    Migration::new(2, "broken-statement", "CREATE TABLE incomplete ("),
];

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn setup_is_idempotent_and_pools_are_schema_scoped() {
    let url = database_url();
    let first_schema = unique_schema("first");
    let second_schema = unique_schema("second");
    let admin = admin_pool(&url).await;

    let first_plan = SchemaPlan::new(first_schema.clone(), V1).unwrap();
    let second_plan = SchemaPlan::new(second_schema.clone(), V1).unwrap();
    let first_operator = SchemaOperator::connect(&url, first_plan.clone())
        .await
        .unwrap();
    assert_eq!(
        first_operator.setup().await.unwrap(),
        SetupOutcome::Created {
            version: 1,
            applied: 1
        }
    );
    assert_eq!(
        first_operator.setup().await.unwrap(),
        SetupOutcome::AlreadyCurrent { version: 1 }
    );
    SchemaOperator::connect(&url, second_plan.clone())
        .await
        .unwrap()
        .setup()
        .await
        .unwrap();

    let first = OwnedPostgres::prepare(&url, first_plan).await.unwrap();
    let second = OwnedPostgres::prepare(&url, second_plan).await.unwrap();
    sqlx::query("INSERT INTO counters (name, value) VALUES ('shared-key', 11)")
        .execute(first.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO counters (name, value) VALUES ('shared-key', 22)")
        .execute(second.pool())
        .await
        .unwrap();
    let first_value: i64 =
        sqlx::query_scalar("SELECT value FROM counters WHERE name = 'shared-key'")
            .fetch_one(first.pool())
            .await
            .unwrap();
    let second_value: i64 =
        sqlx::query_scalar("SELECT value FROM counters WHERE name = 'shared-key'")
            .fetch_one(second.pool())
            .await
            .unwrap();
    assert_eq!((first_value, second_value), (11, 22));

    first.pool().close().await;
    second.pool().close().await;
    cleanup(&admin, &[&first_schema, &second_schema]).await;
}

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn preparation_never_applies_pending_migrations() {
    let url = database_url();
    let schema = unique_schema("upgrade");
    let admin = admin_pool(&url).await;
    let v1 = SchemaPlan::new(schema.clone(), V1).unwrap();
    SchemaOperator::connect(&url, v1.clone())
        .await
        .unwrap()
        .setup()
        .await
        .unwrap();

    let v2 = SchemaPlan::new(schema.clone(), V2).unwrap();
    let error = OwnedPostgres::prepare(&url, v2.clone()).await.unwrap_err();
    assert!(matches!(
        error,
        PostgresKitError::UpgradeRequired {
            current: 1,
            expected: 2,
            ..
        }
    ));
    let column_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns\n\
         WHERE table_schema = $1 AND table_name = 'counters' AND column_name = 'updated_at'",
    )
    .bind(&schema)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(column_count, 0, "prepare must not mutate the schema");

    let outcome = SchemaOperator::connect(&url, v2.clone())
        .await
        .unwrap()
        .upgrade()
        .await
        .unwrap();
    assert_eq!(
        outcome,
        UpgradeOutcome::Applied {
            from: 1,
            to: 2,
            applied: 1
        }
    );
    let prepared = OwnedPostgres::prepare(&url, v2).await.unwrap();
    assert_eq!(prepared.schema_version(), 2);
    prepared.pool().close().await;
    cleanup(&admin, &[&schema]).await;
}

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn checksum_drift_fails_closed() {
    let url = database_url();
    let schema = unique_schema("drift");
    let admin = admin_pool(&url).await;
    let original = SchemaPlan::new(schema.clone(), V1).unwrap();
    SchemaOperator::connect(&url, original)
        .await
        .unwrap()
        .setup()
        .await
        .unwrap();

    let drifted = SchemaPlan::new(schema.clone(), DRIFTED_V1).unwrap();
    assert!(matches!(
        OwnedPostgres::prepare(&url, drifted).await.unwrap_err(),
        PostgresKitError::HistoryDiverged { version: 1, .. }
    ));
    cleanup(&admin, &[&schema]).await;
}

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn unmanaged_schema_is_never_adopted() {
    let url = database_url();
    let schema = unique_schema("unmanaged");
    let admin = admin_pool(&url).await;
    admin
        .execute(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .await
        .unwrap();
    let plan = SchemaPlan::new(schema.clone(), V1).unwrap();
    assert!(matches!(
        SchemaOperator::connect(&url, plan)
            .await
            .unwrap()
            .setup()
            .await
            .unwrap_err(),
        PostgresKitError::UnmanagedSchema { .. }
    ));
    cleanup(&admin, &[&schema]).await;
}

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn failed_setup_rolls_back_schema_and_ledger() {
    let url = database_url();
    let schema = unique_schema("rollback");
    let admin = admin_pool(&url).await;
    let plan = SchemaPlan::new(schema.clone(), BROKEN_SETUP).unwrap();
    assert!(matches!(
        SchemaOperator::connect(&url, plan)
            .await
            .unwrap()
            .setup()
            .await
            .unwrap_err(),
        PostgresKitError::Database {
            operation: "apply owned migration",
            ..
        }
    ));
    let schema_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(&schema)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert!(
        !schema_exists,
        "failed setup must roll back schema creation"
    );
}

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn older_module_rejects_a_newer_schema() {
    let url = database_url();
    let schema = unique_schema("ahead");
    let admin = admin_pool(&url).await;
    let v2 = SchemaPlan::new(schema.clone(), V2).unwrap();
    SchemaOperator::connect(&url, v2)
        .await
        .unwrap()
        .setup()
        .await
        .unwrap();

    let v1 = SchemaPlan::new(schema.clone(), V1).unwrap();
    assert!(matches!(
        OwnedPostgres::prepare(&url, v1).await.unwrap_err(),
        PostgresKitError::SchemaAhead {
            actual: 2,
            expected: 1,
            ..
        }
    ));
    cleanup(&admin, &[&schema]).await;
}

// This fixture represents a ledger written by the original 0.1.0 implementation.
// Its checksum is a fixed historical value, not computed by the extracted core.
#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn legacy_ledger_prepares_and_upgrades_without_rewriting_history() {
    let url = database_url();
    let schema = unique_schema("legacy");
    let admin = admin_pool(&url).await;
    let legacy_checksum = "53f61844516ba5191fac9b3a1f51f594a33b60442c576f9e82a5a97c881dfd3b";
    admin
        .execute(AssertSqlSafe(format!(
            "CREATE SCHEMA \"{schema}\";
         CREATE TABLE \"{schema}\"._lenso_schema_migrations (
             version bigint PRIMARY KEY CHECK (version > 0),
             name text NOT NULL, checksum text NOT NULL,
             applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()
         );
         CREATE TABLE \"{schema}\".counters (name text PRIMARY KEY, value bigint NOT NULL);
         INSERT INTO \"{schema}\"._lenso_schema_migrations (version, name, checksum)
         VALUES (1, 'create-counters', '{legacy_checksum}')"
        )))
        .await
        .unwrap();
    let prepared = OwnedPostgres::prepare(&url, SchemaPlan::new(schema.clone(), V1).unwrap())
        .await
        .unwrap();
    prepared.pool().close().await;
    let plan = SchemaPlan::new(schema.clone(), V2).unwrap();
    assert_eq!(
        SchemaOperator::connect(&url, plan.clone())
            .await
            .unwrap()
            .upgrade()
            .await
            .unwrap(),
        UpgradeOutcome::Applied {
            from: 1,
            to: 2,
            applied: 1
        }
    );
    let checksum: String = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT checksum FROM \"{schema}\"._lenso_schema_migrations WHERE version = 1"
    )))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(checksum, legacy_checksum);
    let prepared = OwnedPostgres::prepare(&url, plan).await.unwrap();
    prepared.pool().close().await;
    cleanup(&admin, &[&schema]).await;
}

#[tokio::test]
#[ignore = "requires LENSO_POSTGRES_TEST_URL"]
async fn ledger_gap_blocks_prepare_and_upgrade() {
    let url = database_url();
    let schema = unique_schema("gap");
    let admin = admin_pool(&url).await;
    let plan = SchemaPlan::new(schema.clone(), V2).unwrap();
    let operator = SchemaOperator::connect(&url, plan.clone()).await.unwrap();
    operator.setup().await.unwrap();
    admin
        .execute(AssertSqlSafe(format!(
            "DELETE FROM \"{schema}\"._lenso_schema_migrations WHERE version = 1"
        )))
        .await
        .unwrap();
    assert!(matches!(
        OwnedPostgres::prepare(&url, plan).await.unwrap_err(),
        PostgresKitError::HistoryDiverged { version: 2, .. }
    ));
    assert!(matches!(
        operator.upgrade().await.unwrap_err(),
        PostgresKitError::HistoryDiverged { version: 2, .. }
    ));
    cleanup(&admin, &[&schema]).await;
}

fn database_url() -> String {
    std::env::var("LENSO_POSTGRES_TEST_URL")
        .expect("LENSO_POSTGRES_TEST_URL must be set for ignored acceptance tests")
}

fn unique_schema(label: &str) -> String {
    let sequence = NEXT_SCHEMA.fetch_add(1, Ordering::Relaxed);
    format!("lenso_kit_{label}_{}_{}", std::process::id(), sequence)
}

async fn admin_pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .unwrap()
}

async fn cleanup(admin: &PgPool, schemas: &[&str]) {
    for schema in schemas {
        admin
            .execute(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .await
            .unwrap();
    }
}
