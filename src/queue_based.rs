// 暂时不考虑dispatcher的唤醒，使dispatcher不睡眠地轮询。

use crate::{
    interface::{IPCSharedEntity, LocalEntityIf, SharedEntityIf},
    vqueue::*,
};
use alloc::{
    collections::btree_map::BTreeMap,
    string::{String, ToString},
    sync::{Arc, Weak},
    vec::Vec,
};
use async_notification::interface::{Notification, NotificationIf};
use core::{
    ops::Deref,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
    usize,
};
use futures::Stream;
use kspin::SpinRaw;

/// 每个该对象持有一个SlotRef的引用计数。
pub struct QueueBasedSharedEntity {
    queue_id: usize,
    // queue: SlotRef<'static, LockFreeDeque<IPCItem, QUEUE_CAPACITY>, ARRAY_LEN>,
}

impl SharedEntityIf for QueueBasedSharedEntity {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        self.queue_id as u64
    }

    unsafe fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized,
    {
        // #[cfg(feature = "log")]
        // log::debug!("QueueBasedSharedEntity::from_id: id=0x{:016x}", id);
        // 增加引用计数
        let slot_ref = unsafe { slotref_from_id(id as usize) };
        #[cfg(feature = "log")]
        // log::debug!(
        //     "QueueBasedSharedEntity::from_id: slot_ref={:?}, rc={} before increase",
        //     slot_ref,
        //     slot_ref.rc()
        // );
        slot_ref.clone().into_id();
        // #[cfg(feature = "log")]
        // log::debug!(
        //     "QueueBasedSharedEntity::from_id: slot_ref={:?}, rc={} after increase",
        //     slot_ref,
        //     slot_ref.rc()
        // );
        slot_ref.into_id();
        Ok(Self {
            queue_id: id as usize,
        })
    }

    /// 发送消息给self
    fn send_to(&self, item: IPCItem) -> Result<(), String> {
        #[cfg(feature = "log")]
        log::debug!(
            "QueueBasedSharedEntity::send_to: queue_id={}",
            self.queue_id
        );
        let res = deque_push(self.queue_id, item).map_err(|_| "send failed".to_string());
        let pid = get_pid(self.queue_id) as u64;
        // 根据pid是否为0判断是否需要通知
        if pid != 0 {
            if let Some(ntf_id) = map_get_ntf_id(self.queue_id, usize::MAX)
                .or_else(|| map_get_ntf_id(self.queue_id, item.msg_type as usize))
            {
                #[cfg(feature = "log")]
                log::debug!(
                    "QueueBasedSharedEntity::send_to: need to notify: pid={}, ntf_id=0x{:016x}",
                    pid,
                    ntf_id
                );
                Notification::notify(pid, ntf_id as u64);
            }
        }
        #[cfg(feature = "log")]
        log::debug!("QueueBasedSharedEntity::send_to result: {:?}", res);
        res
    }
}

impl Drop for QueueBasedSharedEntity {
    fn drop(&mut self) {
        let to_drop = unsafe { slotref_from_id(self.queue_id) };
        // #[cfg(feature = "log")]
        // log::debug!(
        //     "QueueBasedSharedEntity::drop: slot_ref={:?}, rc={} before decrease",
        //     to_drop,
        //     to_drop.rc()
        // );
    }
}

pub struct QueueBasedLocalEntity {
    shared: IPCSharedEntity,
    // slot_ref: SlotRef<'static, LockFreeDeque<IPCItem, QUEUE_CAPACITY>, ARRAY_LEN>,
    /// - true: 使用信号/用户态中断的通知机制唤醒worker
    /// - false: 使用dispatcher唤醒worker
    use_notify: bool,
    /// - true: 通过默认的分配机制接收不同类型的消息。`recv`可用，`recv_any`不可用
    /// - false：不使用默认的分配机制，无法按类型接收消息，只能接收任意类型消息。`recv`不可用，`recv_any`可用
    use_default_dispatcher: bool,
    wait_queue: SpinRaw<BTreeMap<u64, Vec<Weak<Waker>>>>, // 只在同一进程的协程间同步，因此可以使用SpinRaw
    /// key: msg_type,
    ///
    /// value: (sender, rep_type, data)
    immediate_values: SpinRaw<BTreeMap<u64, (u64, u64, [u64; 8])>>,
}

impl QueueBasedLocalEntity {
    /// 构造函数
    ///
    /// 参数：
    ///
    /// - `use_notify`:
    ///     - `true`: 使用信号/用户态中断的通知机制唤醒worker
    ///     - `false`: 使用dispatcher唤醒worker
    /// - `use_default_dispacther`:
    ///     - `true`: 通过默认的分配机制接收不同类型的消息。`recv`可用，`recv_any`不可用
    ///     - `false`：不使用默认的分配机制，无法按类型接收消息，只能接收任意类型消息。`recv`不可用，`recv_any`可用
    /// - pid: 任务调度模块使用的进程id，若use_notify=true则需要传入Some，且需要为非零值。
    pub fn new(
        use_notify: bool,
        use_default_dispatcher: bool,
        pid: Option<usize>,
    ) -> Result<Self, String> {
        let queue = register_process().map_err(|_| "register queue failed.".to_string())?;
        // log::debug!(
        //     "QueueBasedLocalEntity::new: slot_ref={:?}, rc={} after increase",
        //     queue,
        //     queue.rc()
        // );

        let queue_id = queue.into_id();
        if use_notify {
            set_pid(queue_id, pid.unwrap());
        }
        Ok(Self {
            // shared: IPCSharedEntity::QueueBased(unsafe {
            //     QueueBasedSharedEntity::from_id(
            //         // queue.clone().into_id() as u64,
            //         queue.into_id() as u64,
            //     )?
            // }),
            shared: IPCSharedEntity::QueueBased(QueueBasedSharedEntity { queue_id }),
            // slot_ref: queue,
            use_notify,
            use_default_dispatcher,
            wait_queue: SpinRaw::new(BTreeMap::new()),
            immediate_values: SpinRaw::new(BTreeMap::new()),
        })
    }
}

impl LocalEntityIf for QueueBasedLocalEntity {
    async fn recv(&'static self, msg_type: u64) -> Result<IPCItem, String> {
        // 如果`self.use_notify`为`true`，且分配到了通知id
        if let Some(notify_id) = self
            .use_notify
            .then_some(())
            .and_then(|_| Notification::new_id_signal())
        {
            // 先从`immediate_values`中获取，未获取到则注册到等待队列中；
            // 再从IPC队列中获取，未获取到则注册通知唤醒。
            // 由两个唤醒源之一唤醒后，就能获取到IPC消息。
            // 由于取消安全的要求，在这之后会取消另一个唤醒源。
            let res = SelectFuture {
                f1: wait_dispatch(self, msg_type),
                f2: wait_notify(self, msg_type, notify_id),
            }
            .await;
            unsafe {
                Notification::release_id(notify_id);
            }
            res
        } else {
            wait_dispatch(self, msg_type).await
        }
    }

    fn recv_stream(&'static self, msg_type: u64) -> impl Stream<Item = Result<IPCItem, String>> {
        stream
    }
}

impl QueueBasedLocalEntity {
    /// 从self接收任意类型的消息，返回消息类型与消息内容。
    ///
    /// 返回值：OK((msg_type: u64, rep_type: u64, data: [u64; 8]))或Err(String)
    pub async fn recv_any(&self) -> Result<(u64, u64, [u64; 8]), String> {
        if self.use_default_dispatcher {
            return Err("`recv_any` not supported with default dispatcher".to_string());
        }
        let res = self.recv_any_inner().await;
        #[cfg(feature = "log")]
        log::debug!("recv_any: received {:?}", res);
        res.map(|item| (item.msg_type, item.rep_type, item.data))
    }

    pub async fn default_dispatcher(&self) -> ! {
        loop {
            // todo: 传递数据给对应的等待者
            if let Ok(item) = self.recv_any_inner().await {
                #[cfg(feature = "log")]
                log::debug!("default dispatcher recvive: {:?}", item);
                if self
                    .immediate_values
                    .lock()
                    .insert(item.msg_type, (item.sender, item.rep_type, item.data))
                    .is_some()
                {
                    #[cfg(feature = "log")]
                    log::warn!("Overwriting immediate value for msg_type {}", item.msg_type);
                }
                if let Some(waker_list) = self.wait_queue.lock().get_mut(&item.msg_type) {
                    while let Some(waker_weak) = waker_list.pop() {
                        if let Some(waker) = waker_weak.upgrade() {
                            waker.wake_by_ref();
                            #[cfg(feature = "log")]
                            log::debug!("default dispatcher wake a task");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn recv_any_inner(&self) -> Result<IPCItem, String> {
        let queue_id = match &self.shared {
            IPCSharedEntity::QueueBased(ent) => ent.queue_id,
            _ => return Err("invalid shared entity type".to_string()),
        };
        loop {
            // #[cfg(feature = "log")]
            // log::debug!("recv_any_inner loop");
            if let Some(item) = deque_pop(queue_id) {
                // #[cfg(feature = "log")]
                // log::debug!("recv_any_inner return");
                return Ok(item);
            } else {
                // 使用通知唤醒，且分配到了通知源
                if let Some(ntf_id) = self
                    .use_notify
                    .then_some(())
                    .and_then(|_| Notification::new_id_signal())
                {
                    // #[cfg(feature = "log")]
                    // log::debug!("recv_any_inner wait");
                    // 阻塞，等待通知源唤醒
                    map_add_entry(queue_id, usize::MAX, ntf_id as usize).unwrap();
                    Notification::wait_on(ntf_id).await;
                    map_pop_ntf_id(queue_id, usize::MAX).unwrap();
                    YieldNowFuture::new().await;
                } else {
                    // 让出
                    // #[cfg(feature = "log")]
                    // log::debug!("recv_any_inner yield");
                    YieldNowFuture::new().await;
                }
            }
        }
    }

    // async fn recv_inner(&self, msg_type: u64) -> Result<IPCItem, String> {
    //     let queue_id = match &self.shared {
    //         IPCSharedEntity::QueueBased(ent) => ent.queue_id,
    //         _ => return Err("invalid shared entity type".to_string()),
    //     };
    //     let mut waited: bool = false;
    //     loop {
    //         if let Some(item) = deque_pop(queue_id) {
    //             return Ok(item);
    //         } else if let Some(&(sender, rep_type, data)) =
    //             self.immediate_values.lock().get(&msg_type)
    //         {
    //             return Ok(IPCItem {
    //                 sender,
    //                 msg_type,
    //                 rep_type,
    //                 data,
    //             });
    //         } else {
    //             // 目前该代码还有问题：如果消息在此处到达，则未来得及被取出；
    //             // 而消息已经到达，也不会触发之后的通知。
    //             // 因此协程会一直阻塞。
    //             if self.use_notify && !waited {
    //                 // 阻塞，等待通知源唤醒
    //                 if let Some(ntf_id) = Notification::new_id_signal() {
    //                     map_add_entry(queue_id, msg_type as usize, ntf_id as usize).unwrap();
    //                     Notification::wait_on(ntf_id).await;
    //                     map_pop_ntf_id(queue_id, msg_type as usize);
    //                     waited = true;
    //                 } else {
    //                     // 若无法分配到通知源，让出
    //                     YieldNowFuture::new().await;
    //                 }
    //             } else {
    //                 // 让出
    //                 YieldNowFuture::new().await;
    //             }
    //         }
    //     }
    // }
}

impl Deref for QueueBasedLocalEntity {
    type Target = IPCSharedEntity;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

// impl Drop for QueueBasedLocalEntity {
//     fn drop(&mut self) {
//         todo!()
//     }
// }

/// 从`immediate_values`中获取IPC消息，若获取不到则等待其它协程将消息放入`immediate_values`后唤醒。
///
/// 通过将`Waker`的`Arc`存储在`Future`内部（而`wait_queue`中仅存储`Waker`的`Weak`），
/// 实现了自身被`drop`时，由自身注册的`Waker`也会被`drop`。
struct WaitDispatchFuture {
    entity: &'static QueueBasedLocalEntity,
    msg_type: u64,
    waker: Option<Arc<Waker>>,
}

fn wait_dispatch(entity: &'static QueueBasedLocalEntity, msg_type: u64) -> WaitDispatchFuture {
    WaitDispatchFuture {
        entity,
        msg_type,
        waker: None,
    }
}

impl Future for WaitDispatchFuture {
    type Output = Result<IPCItem, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.waker = None;
        if let Some((sender, rep_type, data)) =
            self.entity.immediate_values.lock().remove(&self.msg_type)
        {
            // #[cfg(feature = "log")]
            // log::debug!("WaitDispatchFuture: return Ready");
            Poll::Ready(Ok(IPCItem {
                sender,
                msg_type: self.msg_type,
                rep_type,
                data,
            }))
        } else {
            // #[cfg(feature = "log")]
            // log::debug!("WaitDispatchFuture: will return Pending");
            self.waker = Some(Arc::new(cx.waker().clone()));
            let mut wait_queue = self.entity.wait_queue.lock();
            let waker_list = wait_queue.entry(self.msg_type).or_insert_with(Vec::new);
            waker_list.push(Arc::downgrade(&self.waker.as_ref().unwrap()));
            // #[cfg(feature = "log")]
            // log::debug!("WaitDispatchFuture: return Pending");
            Poll::Pending
        }
    }
}

/// 从队列中获取IPC消息，若获取不到则等待其它进程将消息放入队列后发送通知。
struct WaitNotifyFuture<F: Future> {
    entity: &'static QueueBasedLocalEntity,
    queue_id: usize,
    msg_type: u64,
    ntf_id: u64,
    wait_future: F,
}

fn wait_notify(
    entity: &'static QueueBasedLocalEntity,
    msg_type: u64,
    ntf_id: u64,
) -> WaitNotifyFuture<impl Future> {
    let queue_id = match &entity.shared {
        IPCSharedEntity::QueueBased(ent) => ent.queue_id,
        _ => panic!("invalid shared entity type"),
    };

    #[cfg(feature = "log")]
    log::debug!(
        "wait_notify: before map_add_entry(queue_id={}, msg_type={}, ntf_id=0x{:016x})",
        queue_id,
        msg_type,
        ntf_id
    );
    map_add_entry(queue_id, msg_type as usize, ntf_id as usize).unwrap();
    #[cfg(feature = "log")]
    log::debug!("wait_notify: after map_add_entry");

    WaitNotifyFuture {
        entity,
        queue_id,
        msg_type,
        ntf_id,
        wait_future: Notification::wait_on(ntf_id),
    }
}

impl<F: Future> Future for WaitNotifyFuture<F> {
    type Output = Result<IPCItem, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 先从队列中获取消息
        while let Some(item) = deque_pop(self.queue_id) {
            #[cfg(feature = "log")]
            log::debug!("WaitNotifyFuture::poll: get item {:?}", item);
            if item.msg_type == self.msg_type {
                // 是自己的消息
                return Poll::Ready(Ok(item));
            } else {
                // 把消息放到暂存区，唤醒对应协程
                if self
                    .entity
                    .immediate_values
                    .lock()
                    .insert(item.msg_type, (item.sender, item.rep_type, item.data))
                    .is_some()
                {
                    #[cfg(feature = "log")]
                    log::warn!("Overwriting immediate value for msg_type {}", item.msg_type);
                }
                if let Some(waker_list) = self.entity.wait_queue.lock().get_mut(&item.msg_type) {
                    while let Some(waker_weak) = waker_list.pop() {
                        if let Some(waker) = waker_weak.upgrade() {
                            waker.wake_by_ref();
                            break;
                        }
                    }
                }
            }
        }

        // 再等待通知
        #[cfg(feature = "log")]
        log::debug!("WaitNotifyFuture::poll: before wait");

        let res = unsafe { self.map_unchecked_mut(|s| &mut s.wait_future) }.poll(cx);
        assert!(res.is_pending());
        Poll::Pending
    }
}

impl<F: Future> Drop for WaitNotifyFuture<F> {
    fn drop(&mut self) {
        let res = map_pop_ntf_id(self.queue_id, self.msg_type as usize);
        assert!(res.is_some());
    }
}

struct YieldNowFuture(bool);

/// 实现协程让出
///
/// 不需手动实现`Waker`的`drop`操作，因为`Waker`只会在`poll`内部调用，
/// 而这时`self`一定没有被`drop。`
///
/// 但该`Future`仍不能用于`select`中？
impl YieldNowFuture {
    fn new() -> Self {
        Self(true)
    }
}

impl Future for YieldNowFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            // first poll
            self.get_mut().0 = false;
            cx.waker().wake_by_ref(); // 即使wake后该协程还未返回，下一次poll时也会直接进入else分支，应该在多线程环境下也是安全的？
            Poll::Pending
        } else {
            // second poll
            Poll::Ready(())
        }
    }
}

/// 从两个异步操作中等待最先返回的。较后返回的异步操作会被取消。
///
/// 若两个异步操作在同一次poll中返回Ready，则返回第一个。
///
/// 构建该结构需要两个Future均可以正确地处理异步取消。
struct SelectFuture<F1: Future<Output = O>, F2: Future<Output = O>, O> {
    pub f1: F1,
    pub f2: F2,
}

impl<F1: Future<Output = O>, F2: Future<Output = O>, O> Future for SelectFuture<F1, F2, O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let (f1, f2) = unsafe {
            let f = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut f.f1), Pin::new_unchecked(&mut f.f2))
        };
        if let Poll::Ready(o) = f1.poll(cx) {
            return Poll::Ready(o);
        }
        if let Poll::Ready(o) = f2.poll(cx) {
            return Poll::Ready(o);
        }
        Poll::Pending
    }
}

pub struct SignalRecvStream {
    entity: &'static QueueBasedLocalEntity,
    notify_id: u64,
}

impl Stream for SignalRecvStream {
    type Item = Result<IPCItem, String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // // 目前该函数的实现还有问题：如果消息在此处到达，则未来得及被取出；
        // // 而消息已经到达，也不会触发之后的通知。
        // // 因此协程会一直阻塞。
        // if let Some(item) = deque_pop(self.entity.queue_id) {
        //     Poll::Ready(Some(Ok(item)))
        // } else {
        //     Notification::wait_on(self.notify_id)
        //         .poll_unpin(cx)
        //         .map(|res| {
        //             res.map_err(|e| e.to_string()).map(|_| None) // 唤醒后由`recv`获取消息，因此这里返回None
        //         })
        // }
    }
}
