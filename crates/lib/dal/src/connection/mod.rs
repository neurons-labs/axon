use std::{fmt, time::Duration};

use anyhow::Context;
use sqlx::{
    pool::PoolConnection,
    postgres::{PgConnectOptions, PgPool, PgPoolOptions, Postgres},
};

use crate::{metrics::CONNECTION_METRICS, StorageProcessor};

pub mod holder;

/// Builder for [`ConnectionPool`]s.
pub struct ConnectionPoolBuilder {
    database_url: String,
    max_size: u32,
    statement_timeout: Option<Duration>,
}

impl fmt::Debug for ConnectionPoolBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Database URL is potentially sensitive, thus we omit it.
        formatter
            .debug_struct("ConnectionPoolBuilder")
            .field("max_size", &self.max_size)
            .field("statement_timeout", &self.statement_timeout)
            .finish()
    }
}

impl ConnectionPoolBuilder {
    /// Sets the statement timeout for the pool. See [Postgres docs] for semantics.
    /// If not specified, the statement timeout will not be set.
    ///
    /// [Postgres docs]: https://www.postgresql.org/docs/14/runtime-config-client.html
    pub fn set_statement_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        self.statement_timeout = timeout;
        self
    }

    /// Builds a connection pool from this builder.
    pub async fn build(&self) -> anyhow::Result<ConnectionPool> {
        let options = PgPoolOptions::new().max_connections(self.max_size);
        let mut connect_options: PgConnectOptions = self
            .database_url
            .parse()
            .context("Failed parsing database URL")?;
        if let Some(timeout) = self.statement_timeout {
            let timeout_string = format!("{}s", timeout.as_secs());
            connect_options = connect_options.options([("statement_timeout", timeout_string)]);
        }
        let pool = options
            .connect_with(connect_options)
            .await
            .context("Failed connecting to database")?;
        tracing::info!(
            "Created pool with {max_connections} max connections \
             and {statement_timeout:?} statement timeout",
            max_connections = self.max_size,
            statement_timeout = self.statement_timeout
        );
        Ok(ConnectionPool {
            inner: pool,
            max_size: self.max_size,
        })
    }
}

#[derive(Clone)]
pub struct ConnectionPool {
    pub(crate) inner: PgPool,
    max_size: u32,
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionPool")
            .field("max_size", &self.max_size)
            .finish_non_exhaustive()
    }
}

impl ConnectionPool {
    /// Initializes a builder for connection pools.
    pub fn builder(database_url: &str, max_pool_size: u32) -> ConnectionPoolBuilder {
        ConnectionPoolBuilder {
            database_url: database_url.to_string(),
            max_size: max_pool_size,
            statement_timeout: None,
        }
    }

    /// Initializes a builder for connection pools with a single connection. This is equivalent
    /// to calling `Self::builder(db_url, 1)`.
    pub fn singleton(database_url: &str) -> ConnectionPoolBuilder {
        Self::builder(database_url, 1)
    }

    /// Returns the maximum number of connections in this pool specified during its creation.
    /// This number may be distinct from the current number of connections in the pool (including
    /// idle ones).
    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    /// Creates a `StorageProcessor` entity over a recoverable connection.
    /// Upon a database outage connection will block the thread until
    /// it will be able to recover the connection (or, if connection cannot
    /// be restored after several retries, this will be considered as
    /// irrecoverable database error and result in panic).
    ///
    /// This method is intended to be used in crucial contexts, where the
    /// database access is must-have (e.g. block committer).
    pub async fn access_storage(&self) -> anyhow::Result<StorageProcessor<'_>> {
        self.access_storage_inner(None).await
    }

    /// A version of `access_storage` that would also expose the duration of the connection
    /// acquisition tagged to the `requester` name.
    ///
    /// WARN: This method should not be used if it will result in too many time series (e.g.
    /// from witness generators or provers), otherwise Prometheus won't be able to handle it.
    pub async fn access_storage_tagged(
        &self,
        requester: &'static str,
    ) -> anyhow::Result<StorageProcessor<'_>> {
        self.access_storage_inner(Some(requester)).await
    }

    async fn access_storage_inner(
        &self,
        requester: Option<&'static str>,
    ) -> anyhow::Result<StorageProcessor<'_>> {
        let acquire_latency = CONNECTION_METRICS.acquire.start();
        let conn = self
            .acquire_connection_retried()
            .await
            .context("acquire_connection_retried()")?;
        let elapsed = acquire_latency.observe();
        if let Some(requester) = requester {
            CONNECTION_METRICS.acquire_tagged[&requester].observe(elapsed);
        }
        Ok(StorageProcessor::from_pool(conn))
    }

    async fn acquire_connection_retried(&self) -> anyhow::Result<PoolConnection<Postgres>> {
        const DB_CONNECTION_RETRIES: u32 = 3;
        const BACKOFF_INTERVAL: Duration = Duration::from_secs(1);

        let mut retry_count = 0;
        while retry_count < DB_CONNECTION_RETRIES {
            CONNECTION_METRICS
                .pool_size
                .observe(self.inner.size() as usize);
            CONNECTION_METRICS.pool_idle.observe(self.inner.num_idle());

            let connection = self.inner.acquire().await;
            let connection_err = match connection {
                Ok(connection) => return Ok(connection),
                Err(err) => {
                    retry_count += 1;
                    err
                }
            };

            Self::report_connection_error(&connection_err);
            tracing::warn!(
                "Failed to get connection to DB, backing off for {BACKOFF_INTERVAL:?}: {connection_err}"
            );
            tokio::time::sleep(BACKOFF_INTERVAL).await;
        }

        // Attempting to get the pooled connection for the last time
        match self.inner.acquire().await {
            Ok(conn) => Ok(conn),
            Err(err) => {
                Self::report_connection_error(&err);
                anyhow::bail!("Run out of retries getting a DB connection, last error: {err}");
            }
        }
    }

    fn report_connection_error(err: &sqlx::Error) {
        CONNECTION_METRICS.pool_acquire_error[&err.into()].inc();
    }
}
