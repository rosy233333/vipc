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
    pin::Pin,
    task::{Context, Poll, Waker},
    usize,
};
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
        // 增加引用计数
        let slot_ref = unsafe { slotref_from_id(id as usize) };
        #[cfg(feature = "log")]
        log::debug!("QueueBasedSharedEntity::from_id: slot_ref={:?}", slot_ref);
        slot_ref.clone().into_id();
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
        #[cfg(feature = "log")]
        log::debug!("QueueBasedSharedEntity::send_to result: {:?}", res);
        res
    }
}

impl Drop for QueueBasedSharedEntity {
    fn drop(&mut self) {
        let _to_drop = unsafe { slotref_from_id(self.queue_id) };
    }
}

pub struct QueueBasedLocalEntity {
    shared: IPCSharedEntity,
    // slot_ref: SlotRef<'static, LockFreeDeque<IPCItem, QUEUE_CAPACITY>, ARRAY_LEN>,
    /// - true: 使用信号/用户态中断的通知机制唤醒worker
    /// - false: 使用dispatcher唤醒worker
    use_notify: bool,
    use_default_dispatcher: bool,
    wait_queue: SpinRaw<BTreeMap<u64, Vec<Weak<Waker>>>>, // 只在同一进程的协程间同步，因此可以使用SpinRaw
    /// key: msg_type,
    ///
    /// value: (sender, rep_type, data)
    immediate_values: SpinRaw<BTreeMap<u64, (u64, u64, [u64; 8])>>,
}

// 构造函数
impl QueueBasedLocalEntity {
    pub fn new(use_notify: bool, use_default_dispatcher: bool) -> Result<Self, String> {
        let queue = register_process().map_err(|_| "register queue failed.".to_string())?;
        Ok(Self {
            // shared: IPCSharedEntity::QueueBased(unsafe {
            //     QueueBasedSharedEntity::from_id(
            //         // queue.clone().into_id() as u64,
            //         queue.into_id() as u64,
            //     )?
            // }),
            shared: IPCSharedEntity::QueueBased(QueueBasedSharedEntity {
                queue_id: queue.into_id(),
            }),
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
        WaitIPCFuture {
            entity: self,
            msg_type,
            waker: None,
        }
        .await
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
        self.recv_any_inner()
            .await
            .map(|item| (item.msg_type, item.rep_type, item.data))
    }

    pub async fn default_dispatcher(&self) -> ! {
        loop {
            // todo: 传递数据给对应的等待者
            if let Ok(item) = self.recv_any_inner().await {
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
            if let Some(item) = deque_pop(queue_id) {
                return Ok(item);
            } else {
                if self.use_notify {
                    // 阻塞，等待通知源唤醒
                    if let Some(ntf_id) = Notification::new_id_signal() {
                        map_add_entry(queue_id, usize::MAX, ntf_id as usize).unwrap();
                        Notification::wait_on(ntf_id).await;
                        map_pop_ntf_id(queue_id, usize::MAX).unwrap();
                    } else {
                        // 若无法分配到通知源，让出
                        YieldNowFuture::new().await;
                    }
                } else {
                    // 让出
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

impl Drop for QueueBasedLocalEntity {
    fn drop(&mut self) {
        todo!()
    }
}

/// 通过将`Waker`的`Arc`存储在`Future`内部（而`wait_queue`中仅存储`Waker`的`Weak`），
/// 实现了自身被`drop`时，由自身注册的`Waker`也会被`drop`。
struct WaitIPCFuture {
    entity: &'static QueueBasedLocalEntity,
    msg_type: u64,
    waker: Option<Arc<Waker>>,
}

impl Future for WaitIPCFuture {
    type Output = Result<IPCItem, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.waker = None;
        if let Some((sender, rep_type, data)) =
            self.entity.immediate_values.lock().remove(&self.msg_type)
        {
            Poll::Ready(Ok(IPCItem {
                sender,
                msg_type: self.msg_type,
                rep_type,
                data,
            }))
        } else {
            {
                self.waker = Some(Arc::new(cx.waker().clone()));
                let mut wait_queue = self.entity.wait_queue.lock();
                let waker_list = wait_queue.entry(self.msg_type).or_insert_with(Vec::new);
                waker_list.push(Arc::downgrade(&self.waker.as_ref().unwrap()));
            }
            Poll::Pending
        }
    }
}

struct YieldNowFuture(bool);

/// 实现协程让出
///
/// 不需手动实现`Waker`的`drop`操作，因为`Waker`只会在`poll`内部调用，
/// 而这时`self`一定没有被`drop。`
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
