use diesel::prelude::*;
use diesel::pg::PgConnection;

use diesel::sql_types::Int4;

#[derive(QueryableByName)]
pub struct Health {
    #[sql_type = "Int4"]
    pub response: i32,
}

impl Health {
    pub fn check(conn: &PgConnection) -> bool {
        let responses: Result<Vec<Health>, _> = diesel::sql_query("select 1 as response")
            .load(conn);
        match responses {
            Ok(response) => response.iter().len() == 1 && response[0].response == 1,
            Err(_err) => false
        }
    }
}
