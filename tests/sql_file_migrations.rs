use lenso_postgres_kit::{Migration, SchemaPlan, sql_migrations};

const MIGRATIONS: &[Migration] = sql_migrations![
    (
        1,
        "create-orders",
        "tests/fixtures/migrations/postgres/001_create_orders.sql"
    ),
    (
        2,
        "add-order-status",
        "tests/fixtures/migrations/postgres/002_add_order_status.sql"
    ),
];

#[test]
fn sql_files_form_one_immutable_schema_plan() {
    let plan = SchemaPlan::new("orders_module", MIGRATIONS).unwrap();
    assert_eq!(plan.current_version(), 2);
    assert!(MIGRATIONS[0].sql().contains("CREATE TABLE orders"));
    assert!(MIGRATIONS[1].sql().contains("ADD COLUMN status"));
}
