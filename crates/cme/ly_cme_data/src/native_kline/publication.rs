//! Bounded file publication, separate from the two source aggregation workers.
use anyhow::{anyhow, ensure, Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::{collections::VecDeque, path::PathBuf, sync::Arc, thread::JoinHandle};

const QUEUE: usize = 16;
type Work = Box<dyn FnOnce() -> Result<PathBuf> + Send>;
struct Job {
    work: Work,
    done: Sender<Result<PathBuf>>,
}

pub struct PublicationPool {
    sender: Option<Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
}
impl PublicationPool {
    pub fn new(workers: usize) -> Result<Arc<Self>> {
        ensure!((1..=16).contains(&workers), "publication workers must be 1..16");
        let (sender, receiver) = bounded::<Job>(QUEUE);
        let mut pool = Self { sender: Some(sender), workers: Vec::new() };
        for index in 0..workers {
            let receiver = receiver.clone();
            pool.workers.push(std::thread::Builder::new()
                .name(format!("lseg-publish-{index}"))
                .spawn(move || {
                    while let Ok(job) = receiver.recv() {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job.work))
                            .unwrap_or_else(|_| Err(anyhow!("file publication panicked")));
                        let _ = job.done.send(result);
                    }
                })?);
        }
        Ok(Arc::new(pool))
    }
    fn submit(&self, work: Work) -> Result<Receiver<Result<PathBuf>>> {
        let (done, receiver) = bounded(1);
        self.sender.as_ref().context("publication pool closed")?
            .send(Job { work, done }).map_err(|_| anyhow!("publication workers stopped"))?;
        Ok(receiver)
    }
}
impl Drop for PublicationPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// Per-RIC acknowledgments. Every publication must finish before the RIC is
/// checkpointed; early errors still drain submitted work before leaving it.
pub struct PendingFiles {
    pool: Option<Arc<PublicationPool>>,
    waiting: VecDeque<Receiver<Result<PathBuf>>>,
}
impl PendingFiles {
    pub fn new(pool: Option<Arc<PublicationPool>>) -> Self {
        Self { pool, waiting: VecDeque::new() }
    }
    pub fn execute(&mut self, work: impl FnOnce() -> Result<PathBuf> + Send + 'static) -> Result<()> {
        if self.pool.is_none() {
            work()?;
            return Ok(());
        }
        if self.waiting.len() >= QUEUE {
            self.wait_one()?;
        }
        let receiver = self.pool.as_ref().unwrap().submit(Box::new(work))?;
        self.waiting.push_back(receiver);
        Ok(())
    }
    fn wait_one(&mut self) -> Result<()> {
        if let Some(receiver) = self.waiting.pop_front() {
            receiver.recv().context("file publication acknowledgment lost")??;
        }
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        let mut failure = None;
        while !self.waiting.is_empty() {
            if let Err(error) = self.wait_one() {
                if failure.is_none() { failure = Some(error); }
            }
        }
        match failure { Some(error) => Err(error), None => Ok(()) }
    }
}
impl Drop for PendingFiles {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn failures_and_panics_are_reported_after_all_submitted_files_finish() {
        let pool = PublicationPool::new(2).unwrap();
        let done = Arc::new(AtomicUsize::new(0));
        let mut pending = PendingFiles::new(Some(pool.clone()));
        pending.execute(|| Err(anyhow!("publication failed"))).unwrap();
        let finished = done.clone();
        pending.execute(move || { finished.fetch_add(1, Ordering::SeqCst); Ok("finished".into()) }).unwrap();
        assert!(pending.finish().unwrap_err().to_string().contains("publication failed"));
        assert_eq!(done.load(Ordering::SeqCst), 1);
        pending.execute(|| panic!("test publisher failure")).unwrap();
        assert!(pending.finish().unwrap_err().to_string().contains("panicked"));
        pending.execute(|| Ok("still usable".into())).unwrap();
        pending.finish().unwrap();
    }
}
