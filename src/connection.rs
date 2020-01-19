use diesel::pg::PgConnection;
use std::env;
use std::ops::Deref;
use r2d2;
use r2d2_diesel::ConnectionManager;
use rocket::State;
use rocket::request::{FromRequest, Outcome, Request};
use rocket_contrib::databases::diesel;

pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;

pub struct DbConn(pub r2d2::PooledConnection<ConnectionManager<PgConnection>>);

pub fn init_pool() -> Pool {
    let manager = ConnectionManager::<PgConnection>::new(database_url());
    Pool::builder()
         .max_size(pool_size())
         .build(manager)
         .expect("Failed to initialize database pool")
}

fn database_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must be set")
}

fn pool_size() -> u32 {
    let size = env::var("POOL_SIZE").expect("POOL_SIZE must be set");
    size.parse::<u32>().expect("POOL_SIZE must be an integer")
}

impl <'a, 'r> FromRequest<'a, 'r> for DbConn {
    type Error = ();

    fn from_request(request: &'a Request<'r>) -> Outcome<DbConn, Self::Error> {
        let pool = request.guard::<State<Pool>>()?;
        match pool.get() {
            Ok(conn) => Outcome::Success(DbConn(conn)),
            Err(_) => Outcome::Failure((rocket::http::Status::ServiceUnavailable, ())),
        }
    }
}

impl Deref for DbConn {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
