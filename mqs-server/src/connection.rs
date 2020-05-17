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
use serde::export::{fmt::Display, Formatter};
use std::{env, time::Duration};

/// Type alias for our database connection pool type.
pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;

/// Type alias for our database connection type.
pub type DBConn = PooledConnection<ConnectionManager<PgConnection>>;

/// Error returned if initializing the database connection pool fails.
#[derive(Debug)]
pub enum InitPoolError {
    /// There was an error establishing a connection to the database.
    R2D2(Error),
    /// The given environment variable was missing.
    MissingVariable(&'static str),
    /// The given environment variable contained an invalid value (for example, something which
    /// was not an integer but an integer was expected).
    InvalidValue(&'static str),
}

impl From<Error> for InitPoolError {
    fn from(error: Error) -> Self {
        Self::R2D2(error)
    }
}

impl Display for InitPoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::R2D2(error) => write!(f, "{}", error),
            Self::MissingVariable(name) => write!(f, "{} must be set", name),
            Self::InvalidValue(name) => write!(f, "{} contained an invalid value", name),
        }
    }
}

fn init_pool_builder() -> Result<(Builder<ConnectionManager<PgConnection>>, u16), InitPoolError> {
    let (min_size, max_size) = pool_size()?;
    let pool_builder = Pool::builder()
        .min_idle(Some(u32::from(min_size)))
        .max_size(u32::from(max_size))
        .connection_timeout(Duration::from_secs(1))
        .event_handler(Box::new(ConnectionHandler::new()))
        .error_handler(Box::new(ConnectionHandler::new()));

    Ok((pool_builder, max_size))
}

/// Create a new database pool and connect the minimum required amount of connections.
/// Reads the `DATABASE_URL`, `MIN_POOL_SIZE` and `MAX_POOL_SIZE` environment variables
/// to determine the number of connections and database url to connect to.
/// If `MIN_POOL_SIZE` is not set, `MAX_POOL_SIZE` will be used instead.
///
/// # Errors
///
/// If any of the required variables does not exist, can not be parsed as an integer or does
/// not make sense (min > max). If the minimum number of connections to the database can not be
/// established.
pub fn init_pool_maybe() -> Result<(Pool, u16), InitPoolError> {
    let manager = ConnectionManager::<PgConnection>::new(database_url()?);
    let (pool_builder, max_size) = init_pool_builder()?;
    let pool = pool_builder.build(manager)?;

    Ok((pool, max_size))
}

fn database_url() -> Result<String, InitPoolError> {
    env::var("DATABASE_URL").map_err(|_| InitPoolError::MissingVariable("DATABASE_URL"))
}

fn pool_size() -> Result<(u16, u16), InitPoolError> {
    let max_size = env::var("MAX_POOL_SIZE").map_err(|_| InitPoolError::MissingVariable("MAX_POOL_SIZE"))?;
    let min_size = env::var("MIN_POOL_SIZE").unwrap_or_else(|_| max_size.clone());
    let max_size = max_size
        .parse::<u16>()
        .map_err(|_| InitPoolError::InvalidValue("MAX_POOL_SIZE"))?;
    let min_size = min_size
        .parse::<u16>()
        .map_err(|_| InitPoolError::InvalidValue("MIN_POOL_SIZE"))?;

    Ok((min_size, max_size))
}

#[derive(Debug)]
struct ConnectionHandler {}

impl ConnectionHandler {
    const fn new() -> Self {
        Self {}
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
/// let src = ConnSource {};
/// assert_eq!(src.get(), Some(DbConn {}));
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
        assert_eq!("url://database", &super::database_url().unwrap());
    }

    #[test]
    fn pool() {
        env::set_var("MAX_POOL_SIZE", "50");
        assert_eq!((50, 50), pool_size().unwrap());
        env::set_var("MIN_POOL_SIZE", "20");
        assert_eq!((20, 50), pool_size().unwrap());
        let (_builder, max_size) = init_pool_builder().unwrap();
        assert_eq!(max_size, 50);
    }
}
