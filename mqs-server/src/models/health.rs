use diesel::{prelude::*, sql_types::Int4};
use std::ops::Deref;

use crate::models::PgRepository;

#[derive(QueryableByName)]
struct Health {
    #[sql_type = "Int4"]
    response: i32,
}

pub trait HealthCheckRepository: Send {
    fn check_health(&self) -> bool;
}

impl HealthCheckRepository for PgRepository {
    fn check_health(&self) -> bool {
        let responses: Result<Vec<Health>, _> = diesel::sql_query("select 1 as response").load(self.conn.deref());
        match responses {
            Ok(response) => response.iter().len() == 1 && response[0].response == 1,
            Err(_err) => false,
        }
    }
}
