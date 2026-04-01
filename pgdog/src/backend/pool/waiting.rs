use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::backend::Server;
use tracing::error;

use super::{Error, Guard, Pool, Request};
use tokio::{sync::oneshot::{error::RecvError, *}, time::Instant};

pub(super) struct Waiting {
    pool: Pool,
    rx: Option<Receiver<Result<Box<Server>, Error>>>,
    request: Request,
    waiting: bool,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if self.waiting {
            self.pool.lock().remove_waiter(&self.request.id);
        }
    }
}

/// Wraps a connection receiver to safely handle future cancellation.
///
/// # The Race Condition
///
/// When `put()` dispatches a connection to a waiting client, it:
/// 1. Pops the waiter from the queue
/// 2. Calls `taken.take()` (taken++)
/// 3. Sends the `Box<Server>` via the oneshot channel (`tx.send(Ok(conn))`)
///
/// If the outer `pool.get()` timeout fires *after* step 3 but *before* `rx.await`
/// completes in `wait()`, Tokio cancels the `get_internal` future. This drops the
/// local `rx` (Receiver), which also drops any value already sitting in the channel.
/// The `Box<Server>` is freed (TCP connection closed) — but `pool.checkin()` is
/// **never called**, so the `taken` entry remains permanently as a "phantom".
///
/// After `max_pool_size` phantom entries accumulate the pool is full and every
/// subsequent `pool.get()` times out immediately.
///
/// # The Fix
///
/// `CancellationSafeRx` is armed on creation. When polled to completion it
/// disarms itself so its `Drop` is a no-op. If it is dropped while still armed
/// (i.e. the future was cancelled mid-wait), it calls `try_recv()` on the inner
/// channel: if a `Box<Server>` is already there it calls `pool.checkin()` which
/// removes the phantom `taken` entry and returns the connection to the pool.
struct CancellationSafeRx {
    rx: Receiver<Result<Box<Server>, Error>>,
    pool: Pool,
    armed: bool,
}

impl Drop for CancellationSafeRx {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // The future was cancelled while still waiting. Check whether put() had
        // already sent a connection into the channel (and registered it in taken).
        // If so, return it to the pool to avoid a phantom taken entry.
        match self.rx.try_recv() {
            Ok(Ok(server)) => {
                if let Err(err) = self.pool.checkin(server) {
                    error!("pool checkin error on waiter cancellation: {}", err);
                }
            }
            // Empty (connection not yet sent) or Error (pool sent an error,
            // no Box<Server> involved) — nothing to recover.
            _ => {}
        }
    }
}

impl Future for CancellationSafeRx {
    // Tokio oneshot output: Result<T, RecvError>
    type Output = Result<Result<Box<Server>, Error>, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Receiver<T>: Unpin, so Pin::new is safe.
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(result) => {
                // Value received successfully — disarm so Drop is a no-op.
                self.armed = false;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Waiting {
    /// Create new waiter.
    ///
    /// N.B. You must call and await `Waiting::wait`, otherwise you'll leak waiters.
    ///
    pub(super) fn new(pool: Pool, request: &Request) -> Result<Self, Error> {
        let request = *request;
        let (tx, rx) = channel();

        let full = {
            let mut guard = pool.lock();
            if !guard.online {
                return Err(Error::Offline);
            }
            guard.waiting.push_back(Waiter { request, tx });
            guard.full()
        };

        // Tell maintenance we are in line waiting for a connection.
        if !full {
            pool.comms().request.notify_one();
        }

        Ok(Self {
            pool,
            rx: Some(rx),
            request,
            waiting: true,
        })
    }

    /// Wait for connection from the pool.
    pub(super) async fn wait(&mut self) -> Result<(Guard, Instant), Error> {
        let rx = self.rx.take().expect("waiter rx taken");

        // Wrap in CancellationSafeRx to close the race between put() sending a
        // connection (taken++) and a checkout timeout dropping the receiver before
        // it is read. See CancellationSafeRx documentation for details.
        let safe_rx = CancellationSafeRx {
            rx,
            pool: self.pool.clone(),
            armed: true,
        };

        // Cancellation point: if the outer pool.get() timeout fires here,
        // CancellationSafeRx::drop() recovers any in-flight connection.
        let server = safe_rx.await;

        // Disarm the guard (poll() already did this on success, but be explicit).
        // We cannot be cancelled beyond this point.
        self.waiting = false;

        let now = Instant::now();
        match server {
            Ok(server) => {
                let server = server?;
                Ok((Guard::new(self.pool.clone(), server, now), now))
            }

            Err(_) => {
                // Should not be possible.
                // This means someone else removed my waiter from the wait queue,
                // indicating a bug in the pool.
                Err(Error::CheckoutTimeout)
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct Waiter {
    pub(super) request: Request,
    pub(super) tx: Sender<Result<Box<Server>, Error>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::pool::Pool;
    use crate::net::messages::BackendKeyData;
    use tokio::time::{sleep, timeout, Duration};

    #[tokio::test]
    async fn test_cancellation_safety() {
        let pool = Pool::new_test();
        pool.launch();

        let num_tasks = 10;
        let mut wait_tasks = Vec::new();

        for i in 0..num_tasks {
            let pool_clone = pool.clone();
            let request = Request::unrouted(BackendKeyData::new());
            let mut waiting = Waiting::new(pool_clone, &request).unwrap();

            let wait_task = tokio::spawn(async move { waiting.wait().await });

            wait_tasks.push((wait_task, i));
        }

        {
            let pool_guard = pool.lock();
            assert_eq!(
                pool_guard.waiting.len(),
                num_tasks,
                "All waiters should be in queue"
            );
        }

        sleep(Duration::from_millis(5)).await;

        for (wait_task, i) in wait_tasks {
            if i % 2 == 0 {
                sleep(Duration::from_millis(1)).await;
            }
            wait_task.abort();
        }

        sleep(Duration::from_millis(10)).await;

        let pool_guard = pool.lock();
        assert!(
            pool_guard.waiting.is_empty(),
            "All waiters should be removed from queue on cancellation"
        );
    }

    #[tokio::test]
    async fn test_timeout_removes_waiter() {
        let config = crate::backend::pool::Config {
            inner: pgdog_stats::Config {
                max: 1,
                min: 1,
                checkout_timeout: Duration::from_millis(10),
                ..crate::backend::pool::Config::default().inner
            },
        };

        let pool = Pool::new(&crate::backend::pool::PoolConfig {
            address: crate::backend::pool::Address {
                host: "127.0.0.1".into(),
                port: 5432,
                database_name: "pgdog".into(),
                user: "pgdog".into(),
                password: "pgdog".into(),
                ..Default::default()
            },
            config,
        });
        pool.launch();

        sleep(Duration::from_millis(100)).await;

        let _conn = pool.get(&Request::default()).await.unwrap();

        let request = Request::unrouted(BackendKeyData::new());
        let waiter_pool = pool.clone();
        let get_conn = async move {
            let mut waiting = Waiting::new(waiter_pool.clone(), &request).unwrap();
            waiting.wait().await
        };
        let result = timeout(Duration::from_millis(100), get_conn).await;

        assert!(result.is_err());

        let pool_guard = pool.lock();
        assert!(
            pool_guard.waiting.is_empty(),
            "Waiter should be removed on timeout"
        );
    }
}
