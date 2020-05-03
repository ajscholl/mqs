use hyper::{Body, Request};
use tokio::macros::support::Future;
use uuid::Uuid;

use crate::TRACE_ID_HEADER;

task_local!(
    static TRACE_ID: Uuid;
);

pub fn get_trace_id() -> Option<Uuid> {
    match TRACE_ID.try_with(|trace_id| *trace_id) {
        Err(_) => None,
        Ok(trace_id) => Some(trace_id),
    }
}

pub async fn with_trace_id<F: Future>(id: Uuid, f: F) -> F::Output {
    TRACE_ID.scope(id, f).await
}

pub fn create_trace_id(req: &Request<Body>) -> Uuid {
    TRACE_ID_HEADER.get(req.headers()).unwrap_or_else(|| Uuid::new_v4())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::make_runtime;

    #[test]
    fn get_without_task() {
        assert_eq!(get_trace_id(), None);
    }

    #[test]
    fn get_in_task() {
        let mut rt = make_runtime();
        rt.block_on(async {
            let id = Uuid::new_v4();
            assert_eq!(get_trace_id(), None);
            with_trace_id(id, async move {
                assert_eq!(get_trace_id(), Some(id));
            })
            .await;
        });
    }
}
