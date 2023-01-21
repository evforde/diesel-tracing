use diesel::connection::{
    AnsiTransactionManager, Connection, ConnectionGatWorkaround, SimpleConnection,
    TransactionManager,
};
use diesel::deserialize::{FromSqlRow, StaticallySizedRow};
use diesel::query_builder::{QueryFragment, QueryId};
use diesel::result::{ConnectionResult, QueryResult};
use diesel::serialize::ToSql;
use diesel::sql_types::HasSqlType;
use diesel::sqlite::{Sqlite, SqliteConnection};
use tracing::instrument;

pub struct InstrumentedSqliteConnection {
    inner: SqliteConnection,
}

impl SimpleConnection for InstrumentedSqliteConnection {
    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self, query), err)]
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.inner.batch_execute(query)?;

        Ok(())
    }
}

impl<'conn, 'query> ConnectionGatWorkaround<'conn, 'query, Sqlite>
    for InstrumentedSqliteConnection
{
    type Cursor = <SqliteConnection as ConnectionGatWorkaround<'conn, 'query, Sqlite>>::Cursor;
    type Row = <SqliteConnection as ConnectionGatWorkaround<'conn, 'query, Sqlite>>::Row;
}

impl Connection for InstrumentedSqliteConnection {
    type Backend = Sqlite;
    type TransactionManager = AnsiTransactionManager;

    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(database_url), err)]
    fn establish(database_url: &str) -> ConnectionResult<InstrumentedSqliteConnection> {
        Ok(InstrumentedSqliteConnection {
            inner: SqliteConnection::establish(database_url)?,
        })
    }

    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self, f))]
    fn transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        Self::TransactionManager::transaction(self, f)
    }

    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self, source), err)]
    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Sqlite> + QueryId,
    {
        self.inner.execute_returning_count(source)
    }

    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self))]
    fn transaction_state(&mut self) -> &mut Self::TransactionManager {
        self.inner.transaction_state()
    }
}

impl InstrumentedSqliteConnection {
    /*
    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self, f))]
    pub fn immediate_transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<Error>,
    {
        let f = |conn: &mut SqliteConnection| f(&mut self);
        self.inner.immediate_transaction(f)
    }

    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self, f))]
    pub fn exclusive_transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<Error>,
    {
        let f = |conn: &mut SqliteConnection| f(&mut self);
        self.inner.exclusive_transaction(f)
    }
    */

    #[doc(hidden)]
    #[instrument(fields(db.system="sqlite", otel.kind="client"), skip(self, f))]
    pub fn register_sql_function<ArgsSqlType, RetSqlType, Args, Ret, F>(
        &mut self,
        fn_name: &str,
        deterministic: bool,
        f: F,
    ) -> QueryResult<()>
    where
        F: FnMut(Args) -> Ret + std::panic::UnwindSafe + Send + 'static,
        Args: FromSqlRow<ArgsSqlType, Sqlite> + StaticallySizedRow<ArgsSqlType, Sqlite>,
        Ret: ToSql<RetSqlType, Sqlite>,
        Sqlite: HasSqlType<RetSqlType>,
    {
        self.inner.register_sql_function(fn_name, deterministic, f)
    }
}
