use r2d2::{
    event::{AcquireEvent, CheckinEvent, CheckoutEvent, ReleaseEvent, TimeoutEvent},
    HandleError,
    HandleEvent,
};
use serde::export::fmt::Display;

#[derive(Debug)]
pub struct ConnectionHandler {}

impl ConnectionHandler {
    pub fn new() -> Self {
        ConnectionHandler {}
    }
}

impl HandleEvent for ConnectionHandler {
    fn handle_acquire(&self, event: AcquireEvent) {
        debug!("Acquired new connection {}", event.connection_id());
    }

    fn handle_release(&self, event: ReleaseEvent) {
        debug!("Released connection {}", event.connection_id());
    }

    fn handle_checkout(&self, event: CheckoutEvent) {
        debug!("Checked out connection {}", event.connection_id());
    }

    fn handle_timeout(&self, event: TimeoutEvent) {
        warn!(
            "Getting a connection from the pool timed out after {}ms",
            event.timeout().as_millis()
        );
    }

    fn handle_checkin(&self, event: CheckinEvent) {
        debug!("Returned connection {}", event.connection_id());
    }
}

impl<E: Display> HandleError<E> for ConnectionHandler {
    fn handle_error(&self, error: E) {
        error!("Connection error: {}", error);
    }
}
