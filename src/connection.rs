use diesel::pg::PgConnection;
use std::env;
use std::ops::Deref;
use r2d2;
use r2d2_diesel::ConnectionManager;

pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;

pub struct DbConn(pub r2d2::PooledConnection<ConnectionManager<PgConnection>>);

pub fn init_pool() -> (Pool, u16) {
    let manager = ConnectionManager::<PgConnection>::new(database_url());
    let (min_size, max_size) = pool_size();
    let pool = Pool::builder()
        .min_idle(Some(min_size as u32))
        .max_size(max_size as u32)
        .build(manager)
        .expect("Failed to initialize database pool");

    (pool, max_size)
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
