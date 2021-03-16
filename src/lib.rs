use actix_rt::task::JoinHandle;
use async_trait::async_trait;
use executor_trait::{BlockingExecutor, Executor, FullExecutor, LocalExecutorError, Task};
use lapin::{executor::Executor as OldExecutor, ConnectionProperties};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub trait LapinActixRtExt {
    fn with_actix_rt(self) -> Self
    where
        Self: Sized;
}

impl LapinActixRtExt for ConnectionProperties {
    fn with_actix_rt(self) -> Self {
        self.with_executor(ActixRt)
    }
}

/// Dummy object implementing executor-trait common interfaces on top of tokio
#[derive(Debug, Default, Clone)]
struct ActixRt;

struct ATask(JoinHandle<()>);

impl FullExecutor for ActixRt {}

impl OldExecutor for ActixRt {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> lapin::Result<()> {
        actix_rt::spawn(f);
        Ok(())
    }
}

impl Executor for ActixRt {
    fn block_on(&self, f: Pin<Box<dyn Future<Output = ()>>>) {
        actix_rt::Runtime::new()
            .expect("failed to create runtime")
            .block_on(f);
    }

    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Box<dyn Task> {
        Box::new(ATask(actix_rt::spawn(f)))
    }

    fn spawn_local(
        &self,
        f: Pin<Box<dyn Future<Output = ()>>>,
    ) -> Result<Box<dyn Task>, LocalExecutorError> {
        Err(LocalExecutorError(f))
    }
}

#[async_trait]
impl BlockingExecutor for ActixRt {
    async fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        actix_rt::task::spawn_blocking(f)
            .await
            .expect("blocking task failed");
    }
}

#[async_trait(?Send)]
impl Task for ATask {
    async fn cancel(self: Box<Self>) -> Option<()> {
        self.0.abort();
        self.0.await.ok()
    }
}

impl Future for ATask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                res.expect("task has been canceled");
                Poll::Ready(())
            }
        }
    }
}
