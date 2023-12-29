// Goal: implement a thread pool

// Structure: Threads share access to the pool object, and reach up to grab work
// Each thread waits on the lock to grab work until work is available, grabs the next task,
// releases the lock, and completes th work

use std::any::{Any, TypeId};
use std::thread;
use std::collections::LinkedList;
use std::marker::{Send, Sync};
use std::iter::zip;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread::JoinHandle;

struct TaskHandle<T>
{
    /// Channel receiver which obtains the result of the function executing.
    recv: Receiver<Box<dyn Any + Send + Sync>>,

    /// The only way to test for completion is to poll the channel, so we will save the result for
    /// the appropriate `close_join` call.
    cache_result: Option<T>
}

/// Handle for receiving the value of a function passed to a ThreadPool. Intended to align with
/// JoinHandle in std::thread.
impl<T: Send + Sync + 'static> TaskHandle<T>
{
    /// Generates a new TaskHandle which will listen for the given receive channel. To be used only
    /// by the ThreadPool implementation.
    fn new(rec: Receiver<Box<dyn Any + Send + Sync>>) -> Self
    {
        Self
        {
            recv: rec,
            cache_result: None
        }
    }

    /// Non-blocking method to determine if the referenced task has finished. The result may only
    /// be obtained through the `wait_get` method, and this allows the calling thread to delay or
    /// avoid blocking.
    pub fn is_finished(&mut self) -> bool
    {
        let mut res = self.cache_result.is_some();
        if !res
        {
            let rv = self.recv.try_recv().ok();
            if let Some(val) = rv
            {
                if val.type_id() == TypeId::of::<T>()
                {
                    println!("Type panic!");
                }
                self.cache_result = Some(*val.downcast::<T>().unwrap());
            }
            //self.cache_result = self.recv.try_recv().ok();
            res = self.cache_result.is_some();
        }
        return res;
    }

    /// Obtain the output of the referenced task, blocking if the task is not yet complete.
    pub fn wait_get(self) -> Result<T, &'static str>
    {
        match self.cache_result
        {
            Some(a) => { Ok(a) }
            None => {
                let item = self.recv.recv().unwrap();
                if (&*item).type_id() != TypeId::of::<T>()
                {
                    println!("Type panic! Expected {}", std::any::type_name::<T>());
                }
                Ok(*item.downcast::<T>().expect("type should always match"))
            }
        }
    }
}

struct TaskItem
{
}

struct ThreadPool
{
    /// The communication channel to all of our threads. Send pointers to functions, a flag to keep
    /// alive, and a condition variable to wake everything up.
    input_queue: Arc<(Mutex<(LinkedList<(Sender<Box<dyn Any + Send + Sync>>, Box<dyn FnOnce() -> Box<(dyn Any + Send + Sync)> + Send + Sync + 'static>)>, bool)>, Condvar)>,

    // No particular need to keep this around, but I do anyway.
    // Might add a 'refresh' method to ensure that threads are still alive to process tasks when a
    // task panics.
    num_workers: usize,

    /// Collect the threads when it is time to exit
    join_handles: Vec<JoinHandle<()>>,
}

/// Interface to reuse thread resources to process many tasks. When queueing a task, a `TaskHandle`
/// will be received that provides a means to receive the result of the function, similar to the
/// `JoinHandle` used in the `std::thread` module.
///
/// Tasks are mostly guaranteed to run to completion. The thread pool will not terminate any task
/// in progress, and does not provide a means to terminate tasks prior to their completion.
///
/// The thread pool does not currently have a means to handle tasks that panic during execution, so
/// avoid writing unsafe code.
impl ThreadPool
{
    fn new(workers:usize) -> Self
    {
        let mut s = Self
        {
            num_workers: workers, 
            input_queue: Arc::new((Mutex::new((LinkedList::new(), false)), Condvar::new())),
            join_handles: Vec::with_capacity(workers),
        };

        for _ in 0..workers
        {
            // Clone the Arc and let its ownership move into the new thread
            let queue_ref = s.input_queue.clone();
            // Spawn our thread with logic to monitor for tasks
            let join_handle = thread::spawn(move || {
                loop
                {
                    let (mtx, cv) = &*queue_ref;
                    let mut qh = mtx.lock().unwrap();
                    let mut next_pair = qh.0.pop_front();
                    if next_pair.is_none() && qh.1
                    {
                        // Catch early exits
                        return ();
                    }
                    while next_pair.is_none()
                    {
                        qh = cv.wait(qh).unwrap();
                        next_pair = qh.0.pop_front();
                        if next_pair.is_none() && qh.1
                        {
                            return ();
                        }
                    }
                    if let Some((csend, func)) = next_pair
                    {
                        // This will respond with an Error if the other end has hung up. We do not
                        // want to poison ourselves if this happens, so we ignore issues
                        let _ = csend.send(func());
                    }
                }
            });
            s.join_handles.push(join_handle);
        }
        return s;
    }

    fn queue_fun<T: Send + Sync + 'static, F: (FnOnce() -> T) + Send + Sync + 'static>(&self, fun: F) -> TaskHandle<T>
    {
        let (sender, receiver) = channel();
        let (lock, cv) = &(*self.input_queue);
        let mut qh = lock.lock().unwrap();
        qh.0.push_back(
            (sender,
             Box::new(move || {
                 Box::new(fun()) as Box<(dyn Any + Send + Sync + 'static)>
             }) as Box<(dyn FnOnce() -> Box<(dyn Any + Send + Sync + 'static)> + Send + Sync + 'static)>
            )
            );
        cv.notify_one();
        return TaskHandle::new(receiver);
    }

    fn queue_many<T: Send + Sync + 'static, F: (FnOnce() -> T) + Send + Sync + 'static>(&self, mut func_list: Vec<F>)
        -> Vec<TaskHandle<T>>
    {
        let (lock, cv) = &(*self.input_queue);
        let (sends, mut recvs): (Vec<_>, Vec<_>)
                             = (0..func_list.len()).map(|_| channel()).unzip();
        let boxed = func_list.drain(..)
            .map(|f| Box::new(move || { Box::new(f()) as Box<(dyn Any + Send + Sync + 'static)> }) as Box<(dyn FnOnce() -> Box<(dyn Any + Send + Sync + 'static)> + Send + Sync + 'static)>);
        let pairs = zip(sends, boxed);

        let mut qh = lock.lock().unwrap();
        pairs.for_each(|i| qh.0.push_back(i));
        let res: Vec<TaskHandle<T>> = recvs.drain(..)
            .map(|r| TaskHandle::new(r)).collect();
        cv.notify_all();
        return res;
    }

    /// Signals all threads to stop, but does not block to ensure they are destroyed prior to
    /// continuing.
    /// This does not preclude tasks from being processed. Threads will not terminate until all
    /// tasks are processed.
    fn close(self)
    {
        let (lock, cv) = &(*self.input_queue);
        {
            let mut tup = lock.lock().unwrap();
            tup.1 = true;
        }
        cv.notify_all();
    }

    /// Signals all threads to stop, and blocks until all threads are destroyed. This ensures that
    /// all tasks are completed prior to continuing, as threads will not terminate until all tasks
    /// are completed.
    fn close_join(mut self)
    {
        let (lock, cv) = &(*self.input_queue);
        {
            let mut tup = lock.lock().unwrap();
            tup.1 = true;
        }
        cv.notify_all();

        self.join_handles.drain(..).for_each(|h| { let _ = h.join(); } );
    }
}

fn main()
{
    let tp = ThreadPool::new(8);
    let h1 = tp.queue_fun(|| { 5 + 5 } );
    let h2 = tp.queue_fun(|| { 10 + 10 } );

    let a = h1.wait_get().expect("just go");
    let b = h2.wait_get().expect("just go");

    let mut m: Vec<TaskHandle<i32>> = (1..10).map(|a| tp.queue_fun(move || { a * a } )).collect();

    println!("{}", m.iter_mut().all(|h| h.is_finished()));

    let col: Vec<i32> = m.drain(..).map(|h| h.wait_get().expect("just go")).collect();

    for item in col
    {
        println!("{item}");
    }

    let mut qms = tp.queue_many((1..4).map(|_| { || { 4 + 4 } }).collect());

    let res: Vec<_> = qms.drain(..).map(|h| h.wait_get().expect("result")).collect();
    for item in res
    {
        println!("{item}");
    }

    println!("res: {a}, {b}");
    tp.close_join();
    println!("joined");
}
