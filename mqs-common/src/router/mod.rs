use async_trait::async_trait;
use hyper::{Body, Method, Request, Response};
use std::{collections::hash_map::HashMap, sync::Arc};

mod handler;

pub use handler::handle;

/// A `Handler` represents a single route (Method + Path) a server provides.
#[async_trait]
pub trait Handler<A>: Sync + Send {
    /// A function to determine whether we need to read the body of a request to produce a response.
    /// Defaults to `false`. If you do not return true, `Vec::new()` will be passed to `handle`.
    fn needs_body(&self) -> bool {
        false
    }

    /// Handle a single request. Gets the arguments (like a database connection), the current request,
    /// and the current body (if `needs_body` returned true) to produce a response.
    async fn handle(&self, args: A, req: Request<Body>, body: Vec<u8>) -> Response<Body>
    where
        A: 'async_trait;
}

pub trait WildcardRouter<A>: Sync + Send {
    fn with_segment(&self, segment: &str) -> Router<A>;
}

pub struct Router<A> {
    handler:         HashMap<Method, Arc<dyn Handler<A>>>,
    wildcard_router: Option<Arc<dyn WildcardRouter<A>>>,
    sub_router:      HashMap<&'static str, Router<A>>,
}

impl<A> Router<A> {
    pub fn route<'a, I: Iterator<Item = &'a str>>(
        &self,
        method: &Method,
        mut segments: I,
    ) -> Option<Arc<dyn Handler<A>>> {
        match segments.next() {
            None => match self.handler.get(method) {
                None => None,
                Some(handler) => Some(handler.clone()),
            },
            Some(segment) => {
                if segment.is_empty() {
                    self.route(method, segments)
                } else if let Some(sub) = self.sub_router.get(segment) {
                    sub.route(method, segments)
                } else if let Some(wildcard) = &self.wildcard_router {
                    wildcard.with_segment(segment).route(method, segments)
                } else {
                    None
                }
            },
        }
    }

    pub fn new() -> Self {
        Router {
            handler:         HashMap::new(),
            wildcard_router: None,
            sub_router:      HashMap::new(),
        }
    }

    pub fn new_simple<H: 'static + Handler<A>>(method: Method, handler: H) -> Self {
        Self::new().with_handler(method, handler)
    }

    pub fn with_handler<H: 'static + Handler<A>>(mut self, method: Method, handler: H) -> Self {
        if let Some(_existing) = self.handler.insert(method, Arc::new(handler)) {
            panic!("Can not set handler - already set!");
        }
        self
    }

    pub fn with_wildcard<R: 'static + WildcardRouter<A>>(mut self, handler: R) -> Self {
        if self.wildcard_router.is_some() {
            panic!("Can not set wildcard - already set!");
        }
        self.wildcard_router = Some(Arc::new(handler));
        self
    }

    pub fn with_route(mut self, route: &'static str, router: Router<A>) -> Self {
        if let Some(_existing) = self.sub_router.insert(route, router) {
            panic!("Overwrote existing route {}", route);
        }
        self
    }

    pub fn with_route_simple<H: 'static + Handler<A>>(self, route: &'static str, method: Method, handler: H) -> Self {
        self.with_route(route, Self::new_simple(method, handler))
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::test::make_runtime;
    use hyper::header::HeaderValue;

    struct SimpleHandler;

    #[async_trait]
    impl Handler<()> for SimpleHandler {
        async fn handle(&self, _args: (), _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
            let mut r = Response::new(Body::default());
            r.headers_mut()
                .insert("X-SIMPLE-HANDLER", HeaderValue::from_static("simple"));
            r
        }
    }

    struct StaticHandler {
        message: &'static str,
    }

    #[async_trait]
    impl Handler<()> for StaticHandler {
        async fn handle(&self, _args: (), _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
            let mut r = Response::new(Body::default());
            r.headers_mut()
                .insert("X-STATIC-HANDLER", HeaderValue::from_static(self.message));
            r
        }
    }

    struct CollectingHandler {
        messages: Vec<String>,
    }

    #[async_trait]
    impl Handler<()> for CollectingHandler {
        async fn handle(&self, _args: (), _req: Request<Body>, _body: Vec<u8>) -> Response<Body> {
            let mut r = Response::new(Body::default());
            r.headers_mut().insert(
                "X-COLLECTED",
                HeaderValue::from_str(&format!("{}", self.messages.len())).unwrap(),
            );
            r.headers_mut()
                .insert("X-MESSAGES", HeaderValue::from_str(&self.messages.join(", ")).unwrap());
            r
        }
    }

    impl WildcardRouter<()> for CollectingHandler {
        fn with_segment(&self, segment: &str) -> Router<()> {
            let mut all_messages = self.messages.clone();
            all_messages.push(segment.to_string());
            Router::new_simple(Method::GET, CollectingHandler {
                messages: all_messages.clone(),
            })
            .with_wildcard(CollectingHandler { messages: all_messages })
        }
    }

    #[test]
    fn route_simple_root() {
        let router = Router::new_simple(Method::GET, SimpleHandler);
        let handler = router.route(&Method::GET, vec![""].into_iter()).unwrap();
        let mut rt = make_runtime();
        let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
        assert_eq!(
            response.headers().get("X-SIMPLE-HANDLER"),
            Some(&HeaderValue::from_static("simple"))
        );
    }

    #[test]
    fn route_simple_sub() {
        let router = Router::new().with_route("sub", Router::new_simple(Method::GET, SimpleHandler));
        assert!(router.route(&Method::GET, vec![""].into_iter()).is_none());
        assert!(router.route(&Method::GET, vec!["another"].into_iter()).is_none());
        assert!(router.route(&Method::GET, vec!["another", "sub"].into_iter()).is_none());
        assert!(router.route(&Method::POST, vec!["sub"].into_iter()).is_none());
        let handler = router.route(&Method::GET, vec!["sub"].into_iter()).unwrap();
        let mut rt = make_runtime();
        let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
        assert_eq!(
            response.headers().get("X-SIMPLE-HANDLER"),
            Some(&HeaderValue::from_static("simple"))
        );
    }

    #[test]
    fn route_nested() {
        let router = Router::new().with_route(
            "sub",
            Router::new()
                .with_route("route", Router::new_simple(Method::POST, SimpleHandler))
                .with_route(
                    "static",
                    Router::new_simple(Method::GET, StaticHandler { message: "my message" }),
                )
                .with_handler(Method::GET, StaticHandler {
                    message: "just sub GET",
                })
                .with_handler(Method::POST, StaticHandler {
                    message: "just sub POST",
                }),
        );
        assert!(router.route(&Method::GET, vec![""].into_iter()).is_none());
        assert!(router.route(&Method::DELETE, vec!["sub"].into_iter()).is_none());
        assert!(router.route(&Method::GET, vec!["sub", "another"].into_iter()).is_none());
        assert!(router.route(&Method::GET, vec!["sub", "route"].into_iter()).is_none());
        {
            let handler = router.route(&Method::POST, vec!["sub", "route"].into_iter()).unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-SIMPLE-HANDLER"),
                Some(&HeaderValue::from_static("simple"))
            );
        }
        {
            let handler = router.route(&Method::GET, vec!["sub", "static"].into_iter()).unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-STATIC-HANDLER"),
                Some(&HeaderValue::from_static("my message"))
            );
        }
        {
            let handler = router.route(&Method::GET, vec!["sub"].into_iter()).unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-STATIC-HANDLER"),
                Some(&HeaderValue::from_static("just sub GET"))
            );
        }
        {
            let handler = router.route(&Method::POST, vec!["sub"].into_iter()).unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-STATIC-HANDLER"),
                Some(&HeaderValue::from_static("just sub POST"))
            );
        }
    }

    #[test]
    fn route_wildcard() {
        let router = Router::new()
            .with_route(
                "collect",
                Router::new().with_wildcard(CollectingHandler { messages: Vec::new() }),
            )
            .with_route("simple", Router::new_simple(Method::GET, SimpleHandler));
        assert!(router.route(&Method::POST, vec!["collect"].into_iter()).is_none());
        assert!(router
            .route(&Method::POST, vec!["collect", "a", "b"].into_iter())
            .is_none());
        {
            let handler = router.route(&Method::GET, vec!["simple"].into_iter()).unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-SIMPLE-HANDLER"),
                Some(&HeaderValue::from_static("simple"))
            );
        }
        {
            let handler = router.route(&Method::GET, vec!["collect", "a"].into_iter()).unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-COLLECTED"),
                Some(&HeaderValue::from_static("1"))
            );
            assert_eq!(
                response.headers().get("X-MESSAGES"),
                Some(&HeaderValue::from_static("a"))
            );
        }
        {
            let handler = router
                .route(&Method::GET, vec!["collect", "a", "b"].into_iter())
                .unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-COLLECTED"),
                Some(&HeaderValue::from_static("2"))
            );
            assert_eq!(
                response.headers().get("X-MESSAGES"),
                Some(&HeaderValue::from_static("a, b"))
            );
        }
        {
            let handler = router
                .route(&Method::GET, vec!["collect", "a", "b", "c"].into_iter())
                .unwrap();
            let mut rt = make_runtime();
            let response = rt.block_on(async { handler.handle((), Request::new(Body::default()), Vec::new()).await });
            assert_eq!(
                response.headers().get("X-COLLECTED"),
                Some(&HeaderValue::from_static("3"))
            );
            assert_eq!(
                response.headers().get("X-MESSAGES"),
                Some(&HeaderValue::from_static("a, b, c"))
            );
        }
    }
}
