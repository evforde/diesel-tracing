use diesel::connection::{
    AnsiTransactionManager, Connection, DefaultLoadingMode, LoadRowIter, SimpleConnection,
};
use diesel::connection::{ConnectionGatWorkaround, LoadConnection, TransactionManager};
use diesel::deserialize::Queryable;
use diesel::expression::QueryMetadata;
use diesel::pg::{GetPgMetadataCache, Pg, PgConnection, PgRowByRowLoadingMode};
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult, QueryResult};
use diesel::select;
use diesel::RunQueryDsl;
use tracing::{debug, field, instrument};

// https://www.postgresql.org/docs/12/functions-info.html
// db.name
sql_function!(fn current_database() -> diesel::sql_types::Text);
// net.peer.ip
sql_function!(fn inet_server_addr() -> diesel::sql_types::Inet);
// net.peer.port
sql_function!(fn inet_server_port() -> diesel::sql_types::Integer);
// db.version
sql_function!(fn version() -> diesel::sql_types::Text);

#[derive(Queryable, Clone, Debug, PartialEq)]
struct PgConnectionInfo {
    current_database: String,
    inet_server_addr: ipnetwork::IpNetwork,
    inet_server_port: i32,
    version: String,
}

pub struct InstrumentedPgConnection {
    inner: PgConnection,
    info: PgConnectionInfo,
}

impl SimpleConnection for InstrumentedPgConnection {
    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self, query),
        err,
    )]
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        debug!("executing batch query");
        self.inner.batch_execute(query)?;

        Ok(())
    }
}

impl<'conn, 'query> ConnectionGatWorkaround<'conn, 'query, Pg, DefaultLoadingMode>
    for InstrumentedPgConnection
{
    type Cursor =
        <PgConnection as ConnectionGatWorkaround<'conn, 'query, Pg, DefaultLoadingMode>>::Cursor;
    type Row =
        <PgConnection as ConnectionGatWorkaround<'conn, 'query, Pg, DefaultLoadingMode>>::Row;
}

impl<'conn, 'query> ConnectionGatWorkaround<'conn, 'query, Pg, PgRowByRowLoadingMode>
    for InstrumentedPgConnection
{
    type Cursor =
        <PgConnection as ConnectionGatWorkaround<'conn, 'query, Pg, PgRowByRowLoadingMode>>::Cursor;
    type Row =
        <PgConnection as ConnectionGatWorkaround<'conn, 'query, Pg, PgRowByRowLoadingMode>>::Row;
}

impl Connection for InstrumentedPgConnection {
    type Backend = Pg;
    type TransactionManager = AnsiTransactionManager;

    #[instrument(
        fields(
            db.name=field::Empty,
            db.system="postgresql",
            db.version=field::Empty,
            otel.kind="client",
            net.peer.ip=field::Empty,
            net.peer.port=field::Empty,
        ),
        skip(database_url),
        err,
    )]
    fn establish(database_url: &str) -> ConnectionResult<InstrumentedPgConnection> {
        debug!("establishing postgresql connection");
        let mut conn = PgConnection::establish(database_url)?;

        debug!("querying postgresql connection information");
        let info: PgConnectionInfo = select((
            current_database(),
            inet_server_addr(),
            inet_server_port(),
            version(),
        ))
        .get_result(&mut conn)
        .map_err(ConnectionError::CouldntSetupConfiguration)?;

        let span = tracing::Span::current();
        span.record("db.name", &info.current_database.as_str());
        span.record("db.version", &info.version.as_str());
        span.record(
            "net.peer.ip",
            &format!("{}", info.inet_server_addr).as_str(),
        );
        span.record("net.peer.port", &info.inet_server_port);

        Ok(InstrumentedPgConnection { inner: conn, info })
    }

    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self, f),
    )]
    fn transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        Self::TransactionManager::transaction(self, f)
    }

    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self, source),
        err,
    )]
    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Pg> + QueryId,
    {
        self.inner.execute_returning_count(source)
    }

    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self),
    )]
    fn transaction_state(&mut self) -> &mut Self::TransactionManager {
        self.inner.transaction_state()
    }
}

impl LoadConnection<DefaultLoadingMode> for InstrumentedPgConnection {
    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self, source),
        err,
    )]
    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<LoadRowIter<'conn, 'query, PgConnection, Pg, DefaultLoadingMode>>
    where
        T: Query + QueryFragment<Pg> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        <PgConnection as LoadConnection<DefaultLoadingMode>>::load(&mut self.inner, source)
    }
}

impl LoadConnection<PgRowByRowLoadingMode> for InstrumentedPgConnection {
    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self, source),
        err,
    )]
    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<LoadRowIter<'conn, 'query, PgConnection, Pg, PgRowByRowLoadingMode>>
    where
        T: Query + QueryFragment<Pg> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        <PgConnection as LoadConnection<PgRowByRowLoadingMode>>::load(&mut self.inner, source)
    }
}

impl GetPgMetadataCache for InstrumentedPgConnection {
    fn get_metadata_cache(&mut self) -> &mut diesel::pg::PgMetadataCache {
        self.inner.get_metadata_cache()
    }
}

/*
impl InstrumentedPgConnection {
    #[instrument(
        fields(
            db.name=%self.info.current_database,
            db.system="postgresql",
            db.version=%self.info.version,
            otel.kind="client",
            net.peer.ip=%self.info.inet_server_addr,
            net.peer.port=%self.info.inet_server_port,
        ),
        skip(self),
    )]
    pub fn build_transaction(&self) -> TransactionBuilder {
        debug!("starting transaction builder");
        self.inner.build_transaction()
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_info_on_establish() {
        InstrumentedPgConnection::establish(
            &std::env::var("POSTGRESQL_URL").expect("no POSTGRESQL_URL env var specified"),
        )
        .expect("failed to establish connection or collect info");
    }
}
