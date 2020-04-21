use diesel::pg::PgConnection;
use r2d2::{self, Error};
use r2d2_diesel::ConnectionManager;
use std::{env, ops::Deref, time::Duration};

use crate::logger::connection::ConnectionHandler;

pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;

pub struct DbConn(pub r2d2::PooledConnection<ConnectionManager<PgConnection>>);

pub fn init_pool_maybe() -> Result<(Pool, u16), Error> {
    let manager = ConnectionManager::<PgConnection>::new(database_url());
    let (min_size, max_size) = pool_size();
    let pool = Pool::builder()
        .min_idle(Some(min_size as u32))
        .max_size(max_size as u32)
        .connection_timeout(Duration::from_secs(1))
        .event_handler(Box::new(ConnectionHandler::new()))
        .error_handler(Box::new(ConnectionHandler::new()))
        .build(manager)?;

    Ok((pool, max_size))
}

pub fn init_pool() -> (Pool, u16) {
    init_pool_maybe().expect("Failed to initialize database pool")
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

impl Deref for DbConn {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
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
    fn pool_size() {
        env::set_var("MAX_POOL_SIZE", "50");
        assert_eq!((50, 50), super::pool_size());
        env::set_var("MIN_POOL_SIZE", "20");
        assert_eq!((20, 50), super::pool_size());
    }
}
