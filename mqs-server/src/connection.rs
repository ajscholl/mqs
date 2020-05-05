use diesel::pg::PgConnection;
use r2d2::{
    self,
    event::{AcquireEvent, CheckinEvent, CheckoutEvent, ReleaseEvent, TimeoutEvent},
    Builder,
    Error,
    HandleError,
    HandleEvent,
    PooledConnection,
};
use r2d2_diesel::ConnectionManager;
use serde::export::fmt::Display;
use std::{env, time::Duration};

/// Type alias for our database connection pool type.
pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;

/// Type alias for our database connection type.
pub type DBConn = PooledConnection<ConnectionManager<PgConnection>>;

fn init_pool_builder() -> (Builder<ConnectionManager<PgConnection>>, u16) {
    let (min_size, max_size) = pool_size();
    let pool_builder = Pool::builder()
        .min_idle(Some(min_size as u32))
        .max_size(max_size as u32)
        .connection_timeout(Duration::from_secs(1))
        .event_handler(Box::new(ConnectionHandler::new()))
        .error_handler(Box::new(ConnectionHandler::new()));

    (pool_builder, max_size)
}

/// Create a new database pool and connect the minimum required amount of connections.
/// If
pub fn init_pool_maybe() -> Result<(Pool, u16), Error> {
    let manager = ConnectionManager::<PgConnection>::new(database_url());
    let (pool_builder, max_size) = init_pool_builder();
    let pool = pool_builder.build(manager)?;

    Ok((pool, max_size))
}

fn database_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must be set")
}

fn pool_size() -> (u16, u16) {
    let max_size = env::var("MAX_POOL_SIZE").expect("MAX_POOL_SIZE must be set");
    let min_size = env::var("MIN_POOL_SIZE").unwrap_or_else(|_| max_size.clone());
    let max_size = max_size.parse::<u16>().expect("MAX_POOL_SIZE must be an integer");
    let min_size = min_size.parse::<u16>().expect("MIN_POOL_SIZE must be an integer");

    (min_size, max_size)
}

#[derive(Debug)]
struct ConnectionHandler {}

impl ConnectionHandler {
    fn new() -> Self {
        ConnectionHandler {}
    }
}

impl HandleEvent for ConnectionHandler {
    fn handle_acquire(&self, event: AcquireEvent) {
        debug!("Acquired new connection {}", event.connection_id());
    }

    fn handle_release(&self, event: ReleaseEvent) {
        debug!("Released connection {}", event.connection_id());
    }

    fn handle_checkout(&self, event: CheckoutEvent) {
        debug!("Checked out connection {}", event.connection_id());
    }

    fn handle_timeout(&self, event: TimeoutEvent) {
        warn!(
            "Getting a connection from the pool timed out after {}ms",
            event.timeout().as_millis()
        );
    }

    fn handle_checkin(&self, event: CheckinEvent) {
        debug!("Returned connection {}", event.connection_id());
    }
}

impl<E: Display> HandleError<E> for ConnectionHandler {
    fn handle_error(&self, error: E) {
        error!("Connection error: {}", error);
    }
}

/// A `Source` can be used to get (potentially) scarce resources like database connections.
///
/// ```
/// use mqs_server::connection::Source;
///
/// #[derive(Debug, PartialEq)]
/// struct DbConn {}
///
/// struct ConnSource {}
///
/// impl Source<DbConn> for ConnSource {
///     fn get(&self) -> Option<DbConn> {
///         Some(DbConn {})
///     }
/// }
///
/// fn main() {
///     let src = ConnSource {};
///     assert_eq!(src.get(), Some(DbConn {}));
/// }
/// ```
pub trait Source<R>: Send {
    /// Get a resource from a `Source`.
    fn get(&self) -> Option<R>;
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn database_url() {
        env::set_var("DATABASE_URL", "url://database");
        assert_eq!("url://database", &super::database_url());
    }

    #[test]
    fn pool() {
        env::set_var("MAX_POOL_SIZE", "50");
        assert_eq!((50, 50), pool_size());
        env::set_var("MIN_POOL_SIZE", "20");
        assert_eq!((20, 50), pool_size());
        let (_builder, max_size) = init_pool_builder();
        assert_eq!(max_size, 50);
    }
}
