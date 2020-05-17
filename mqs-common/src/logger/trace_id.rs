use hyper::{Body, Request};
use tokio::macros::support::Future;
use uuid::Uuid;

use crate::TraceIdHeader;

task_local!(
    static TRACE_ID: Uuid;
);

/// Get the current trace id of the current task if any was set via `with_trace_id`.
///
/// ```
/// use mqs_common::{
///     logger::{get_trace_id, with_trace_id},
///     test::make_runtime,
/// };
/// use uuid::Uuid;
///
/// make_runtime().block_on(async {
///     assert_eq!(get_trace_id(), None);
///     let trace_id = Uuid::new_v4();
///     with_trace_id(trace_id, async {
///         assert_eq!(get_trace_id(), Some(trace_id));
///     })
///     .await;
/// });
/// ```
#[must_use]
pub fn get_trace_id() -> Option<Uuid> {
    match TRACE_ID.try_with(|trace_id| *trace_id) {
        Err(_) => None,
        Ok(trace_id) => Some(trace_id),
    }
}

/// Execute the given future with the trace id set to the given value. The trace id will be cleared after
/// the future ends.
///
/// ```
/// use mqs_common::{
///     logger::{get_trace_id, with_trace_id},
///     test::make_runtime,
/// };
/// use uuid::Uuid;
///
/// make_runtime().block_on(async {
///     let outer = Uuid::new_v4();
///     with_trace_id(outer, async {
///         assert_eq!(get_trace_id(), Some(outer));
///         let inner = Uuid::new_v4();
///         with_trace_id(inner, async {
///             assert_eq!(get_trace_id(), Some(inner));
///         })
///         .await;
///         assert_eq!(get_trace_id(), Some(outer));
///     })
///     .await;
///     assert_eq!(get_trace_id(), None);
/// });
/// ```
pub async fn with_trace_id<F: Future + Send>(id: Uuid, f: F) -> F::Output {
    TRACE_ID.scope(id, f).await
}

/// Extract a trace id from a request. If the 'X-TRACE-ID` header is set, we accept the trace id
/// of the request. Otherwise we generatea fresh one.
///
/// ```
/// use http::{HeaderValue, Request};
/// use hyper::Body;
/// use mqs_common::{logger::create_trace_id, TraceIdHeader};
///
/// let mut req = Request::new(Body::default());
/// // random trace id as we did not supply any headers
/// let trace_id = create_trace_id(&req);
/// req.headers_mut().insert(
///     TraceIdHeader::name(),
///     HeaderValue::from_str(&trace_id.to_string()).unwrap(),
/// );
/// // we get back whatever we set as X-TRACE-ID header
/// assert_eq!(create_trace_id(&req), trace_id);
/// ```
pub fn create_trace_id(req: &Request<Body>) -> Uuid {
    TraceIdHeader::get(req.headers()).unwrap_or_else(Uuid::new_v4)
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
