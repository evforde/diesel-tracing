use diesel::connection::{
    AnsiTransactionManager, Connection, ConnectionGatWorkaround, DefaultLoadingMode,
    LoadConnection, LoadRowIter, SimpleConnection, TransactionManager,
};
use diesel::expression::QueryMetadata;
use diesel::mysql::{Mysql, MysqlConnection};
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::result::{ConnectionResult, QueryResult};
use tracing::instrument;

pub struct InstrumentedMysqlConnection {
    inner: MysqlConnection,
}

impl SimpleConnection for InstrumentedMysqlConnection {
    #[instrument(fields(db.system="mysql", otel.kind="client"), skip(self, query), err)]
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.inner.batch_execute(query)?;

        Ok(())
    }
}

impl<'conn, 'query> ConnectionGatWorkaround<'conn, 'query, Mysql> for InstrumentedMysqlConnection {
    type Cursor = <MysqlConnection as ConnectionGatWorkaround<'conn, 'query, Mysql>>::Cursor;
    type Row = <MysqlConnection as ConnectionGatWorkaround<'conn, 'query, Mysql>>::Row;
}

impl Connection for InstrumentedMysqlConnection {
    type Backend = Mysql;
    type TransactionManager = AnsiTransactionManager;

    #[instrument(fields(db.system="mysql", otel.kind="client"), skip(database_url), err)]
    fn establish(database_url: &str) -> ConnectionResult<InstrumentedMysqlConnection> {
        Ok(InstrumentedMysqlConnection {
            inner: MysqlConnection::establish(database_url)?,
        })
    }

    #[instrument(fields(db.system="mysql", otel.kind="client"), skip(self, f))]
    fn transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        Self::TransactionManager::transaction(self, f)
    }

    #[instrument(fields(db.system="mysql", otel.kind="client"), skip(self, source), err)]
    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Mysql> + QueryId,
    {
        self.inner.execute_returning_count(source)
    }

    #[instrument(fields(db.system="mysql", otel.kind="client"), skip(self))]
    fn transaction_state(&mut self) -> &mut Self::TransactionManager {
        self.inner.transaction_state()
    }
}

impl LoadConnection<DefaultLoadingMode> for InstrumentedMysqlConnection {
    #[instrument(fields(db.system="mysql", otel.kind="client"), skip(self, source), err)]
    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<LoadRowIter<'conn, 'query, Self, Self::Backend>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        self.inner.load(source)
    }
}
