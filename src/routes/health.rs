use crate::models::health::Health;
use crate::connection::DbConn;

#[get("/health")]
pub fn health(conn: DbConn) -> &'static str {
    if Health::check(&conn) {
        "green"
    } else {
        "red"
    }
}
