use futures::{executor::block_on, future::LocalBoxFuture};
use lenso_migration::Migration;
use lenso_migration_d1::{Error, LegacySchema, Plan, SqlMigration, Statement, Transport};
use rusqlite::{Connection, params_from_iter, types::Value as SqlValue};
use serde_json::{Value, json};
use std::cell::{Cell, RefCell};

const INITIAL: &str = "CREATE TABLE example(id INTEGER PRIMARY KEY);";
const NEXT: &str = "ALTER TABLE example ADD COLUMN label TEXT;";
static COMMON: [Migration; 2] = [
    Migration::new(1, "create-example", INITIAL),
    Migration::new(2, "add-label", NEXT),
];
static SQL: [SqlMigration; 2] = [
    SqlMigration {
        migration: COMMON[0],
        statement_ends: &[INITIAL.len()],
    },
    SqlMigration {
        migration: COMMON[1],
        statement_ends: &[NEXT.len()],
    },
];
fn plan(count: usize) -> Plan {
    Plan::new("example", &SQL[..count], &COMMON[..count], None).unwrap()
}

struct Db {
    conn: RefCell<Connection>,
    writes: Cell<usize>,
    lose_reply: Cell<bool>,
    stale: Cell<bool>,
    fresh_race: Cell<bool>,
    marker_race: Cell<bool>,
}
impl Db {
    fn new() -> Self {
        Self {
            conn: RefCell::new(Connection::open_in_memory().unwrap()),
            writes: Cell::new(0),
            lose_reply: Cell::new(false),
            stale: Cell::new(false),
            fresh_race: Cell::new(false),
            marker_race: Cell::new(false),
        }
    }
    fn sql(&self, sql: &str) {
        self.conn.borrow().execute_batch(sql).unwrap();
    }
}
impl Transport for Db {
    fn batch(
        &self,
        statements: Vec<Statement>,
    ) -> LocalBoxFuture<'_, Result<Vec<Vec<Value>>, Error>> {
        Box::pin(async move {
            let mut conn = self.conn.borrow_mut();
            let writing = statements.len() > 1;
            if writing {
                self.writes.set(self.writes.get() + 1);
                if self.stale.replace(false) {
                    conn.execute("UPDATE _lenso_migrations SET checksum='changed'", [])
                        .unwrap();
                }
                if self.fresh_race.replace(false) {
                    conn.execute_batch("CREATE TABLE foreign_data(id INTEGER); CREATE TABLE _lenso_migrations(owner TEXT,backend TEXT,version INTEGER,name TEXT,checksum TEXT,PRIMARY KEY(owner,backend,version)); INSERT INTO _lenso_migrations VALUES('foreign','d1',1,'create-foreign','checksum')").unwrap();
                }
                if self.marker_race.replace(false) {
                    conn.execute("UPDATE legacy SET fingerprint='changed'", [])
                        .unwrap();
                }
            }
            let tx = conn.transaction().map_err(|_| Error::Transport)?;
            let mut output = vec![];
            for statement in statements {
                let mut stmt = tx.prepare(&statement.sql).map_err(|_| Error::Transport)?;
                let names = stmt
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let params = statement
                    .params
                    .into_iter()
                    .map(|v| match v {
                        Value::Null => SqlValue::Null,
                        Value::Number(n) => SqlValue::Integer(n.as_i64().unwrap()),
                        Value::String(s) => SqlValue::Text(s),
                        _ => panic!("unsupported fixture parameter"),
                    })
                    .collect::<Vec<_>>();
                let mut rows = stmt
                    .query(params_from_iter(params))
                    .map_err(|_| Error::Transport)?;
                let mut values = vec![];
                while let Some(row) = rows.next().map_err(|_| Error::Transport)? {
                    let mut object = serde_json::Map::new();
                    for (i, name) in names.iter().enumerate() {
                        let value = match row.get::<_, SqlValue>(i).unwrap() {
                            SqlValue::Null => Value::Null,
                            SqlValue::Integer(n) => json!(n),
                            SqlValue::Text(s) => json!(s),
                            _ => panic!("unsupported fixture value"),
                        };
                        object.insert(name.clone(), value);
                    }
                    values.push(Value::Object(object));
                }
                output.push(values);
            }
            tx.commit().map_err(|_| Error::Transport)?;
            if writing && self.lose_reply.replace(false) {
                return Err(Error::Transport);
            }
            Ok(output)
        })
    }
}

#[test]
fn setup_upgrade_and_prepare_are_explicit() {
    block_on(async {
        let db = Db::new();
        assert!(matches!(
            plan(1).verify(&db).await,
            Err(Error::SetupRequired)
        ));
        assert_eq!(db.writes.get(), 0);
        plan(1).setup(&db).await.unwrap();
        plan(1).setup(&db).await.unwrap();
        assert_eq!(db.writes.get(), 1);
        assert!(matches!(
            plan(2).verify(&db).await,
            Err(Error::UpgradeRequired)
        ));
        assert!(matches!(
            plan(2).setup(&db).await,
            Err(Error::UpgradeRequired)
        ));
        plan(2).upgrade(&db).await.unwrap();
        plan(2).verify(&db).await.unwrap();
        assert!(matches!(plan(1).verify(&db).await, Err(Error::History)));
    });
}

#[test]
fn checksum_and_owner_mismatches_fail_closed() {
    block_on(async {
        let db = Db::new();
        plan(1).setup(&db).await.unwrap();
        let other = Plan::new("other", &SQL[..1], &COMMON[..1], None).unwrap();
        assert!(matches!(other.verify(&db).await, Err(Error::SetupRequired)));
        assert!(matches!(other.setup(&db).await, Err(Error::Unmanaged)));
        db.sql("UPDATE _lenso_migrations SET checksum='bad'");
        assert!(matches!(plan(1).verify(&db).await, Err(Error::History)));
    });
}

#[test]
fn stale_plan_guard_precedes_schema_mutation() {
    block_on(async {
        let db = Db::new();
        plan(1).setup(&db).await.unwrap();
        db.stale.set(true);
        assert!(matches!(plan(2).upgrade(&db).await, Err(Error::Transport)));
        assert!(
            db.conn
                .borrow()
                .prepare("SELECT label FROM example")
                .is_err()
        );
    });
}

#[test]
fn failed_batch_rolls_back_schema_and_history() {
    block_on(async {
        const BAD: &str = "INSERT INTO missing VALUES(1);";
        static COMMON_BAD: [Migration; 2] = [COMMON[0], Migration::new(2, "bad-step", BAD)];
        static SQL_BAD: [SqlMigration; 2] = [
            SQL[0],
            SqlMigration {
                migration: COMMON_BAD[1],
                statement_ends: &[BAD.len()],
            },
        ];
        let bad = Plan::new("example", &SQL_BAD, &COMMON_BAD, None).unwrap();
        let db = Db::new();
        assert!(bad.setup(&db).await.is_err());
        assert!(db.conn.borrow().prepare("SELECT * FROM example").is_err());
        assert!(matches!(bad.verify(&db).await, Err(Error::SetupRequired)));
    });
}

#[test]
fn lost_write_receipt_is_not_retried() {
    block_on(async {
        let db = Db::new();
        db.lose_reply.set(true);
        assert!(matches!(plan(1).setup(&db).await, Err(Error::Transport)));
        assert_eq!(db.writes.get(), 1);
        plan(1).verify(&db).await.unwrap();
        plan(1).setup(&db).await.unwrap();
        assert_eq!(db.writes.get(), 1);
    });
}

#[test]
fn legacy_adoption_verifies_marker_without_replaying_sql() {
    block_on(async {
        let db = Db::new();
        db.sql("CREATE TABLE legacy(version INTEGER PRIMARY KEY,fingerprint TEXT);INSERT INTO legacy VALUES(1,'expected');CREATE TABLE example(id INTEGER PRIMARY KEY);INSERT INTO example VALUES(7)");
        let legacy = Plan::new(
            "example",
            &SQL[..1],
            &COMMON[..1],
            Some(LegacySchema {
                table: "legacy",
                fingerprint: "expected",
            }),
        )
        .unwrap();
        assert!(matches!(legacy.setup(&db).await, Err(Error::Unmanaged)));
        legacy.adopt_legacy(&db).await.unwrap();
        legacy.verify(&db).await.unwrap();
        let count: i64 = db
            .conn
            .borrow()
            .query_row("SELECT count(*) FROM example", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        let wrong = Db::new();
        wrong.sql("CREATE TABLE legacy(version INTEGER PRIMARY KEY,fingerprint TEXT);INSERT INTO legacy VALUES(1,'wrong')");
        assert!(legacy.adopt_legacy(&wrong).await.is_err());
        assert!(matches!(
            legacy.verify(&wrong).await,
            Err(Error::SetupRequired)
        ));
    });
}

#[test]
fn fresh_setup_rechecks_all_owners_and_tables_in_its_write_batch() {
    block_on(async {
        let db = Db::new();
        db.fresh_race.set(true);
        assert!(matches!(plan(1).setup(&db).await, Err(Error::Transport)));
        assert!(db.conn.borrow().prepare("SELECT * FROM example").is_err());
        let owners: Vec<String> = db
            .conn
            .borrow()
            .prepare("SELECT owner FROM _lenso_migrations")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(owners, ["foreign"]);
    });
}

#[test]
fn similarly_named_user_tables_are_not_treated_as_d1_internal_tables() {
    block_on(async {
        for name in ["acfx", "sqliteXdata"] {
            let db = Db::new();
            db.sql(&format!("CREATE TABLE {name}(id INTEGER)"));
            assert!(matches!(plan(1).setup(&db).await, Err(Error::Unmanaged)));
            assert_eq!(db.writes.get(), 0);
        }
    });
}

#[test]
fn other_owner_history_blocks_setup_even_without_user_tables() {
    block_on(async {
        let db = Db::new();
        db.sql("CREATE TABLE _lenso_migrations(owner TEXT,backend TEXT,version INTEGER,name TEXT,checksum TEXT,PRIMARY KEY(owner,backend,version)); INSERT INTO _lenso_migrations VALUES('foreign','d1',1,'create-foreign','checksum')");
        assert!(matches!(plan(1).setup(&db).await, Err(Error::Transport)));
        assert!(db.conn.borrow().prepare("SELECT * FROM example").is_err());
    });
}

#[test]
fn upgrade_rechecks_legacy_marker_before_mutation() {
    block_on(async {
        let db = Db::new();
        db.sql("CREATE TABLE legacy(version INTEGER PRIMARY KEY,fingerprint TEXT);INSERT INTO legacy VALUES(1,'expected');CREATE TABLE example(id INTEGER PRIMARY KEY)");
        let legacy = Plan::new(
            "example",
            &SQL,
            &COMMON,
            Some(LegacySchema {
                table: "legacy",
                fingerprint: "expected",
            }),
        )
        .unwrap();
        legacy.adopt_legacy(&db).await.unwrap();
        db.marker_race.set(true);
        assert!(matches!(legacy.upgrade(&db).await, Err(Error::Transport)));
        assert!(
            db.conn
                .borrow()
                .prepare("SELECT label FROM example")
                .is_err()
        );
        let versions: Vec<i64> = db
            .conn
            .borrow()
            .prepare("SELECT version FROM _lenso_migrations")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(versions, [1]);
    });
}

#[test]
fn history_growth_does_not_exhaust_bound_parameters() {
    block_on(async {
        let common: &'static [Migration] = Box::leak(
            (1..=41)
                .map(|version| Migration::new(version, "step", "SELECT 1;"))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let statements: &'static [SqlMigration] = Box::leak(
            common
                .iter()
                .map(|&migration| SqlMigration {
                    migration,
                    statement_ends: &[9],
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let first = Plan::new("history", &statements[..40], &common[..40], None).unwrap();
        let next = Plan::new("history", statements, common, None).unwrap();
        let db = Db::new();
        first.setup(&db).await.unwrap();
        next.upgrade(&db).await.unwrap();
        next.verify(&db).await.unwrap();
    });
}
