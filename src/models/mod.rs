use crate::connection::DbConn;

pub mod health;
pub mod message;
pub mod queue;

pub struct PgRepository {
    conn: DbConn,
}

impl PgRepository {
    pub fn new(conn: DbConn) -> Self {
        PgRepository { conn }
    }
}
