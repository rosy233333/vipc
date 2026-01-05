// 暂时不考虑dispatcher的唤醒，使dispatcher不睡眠地轮询。

use crate::{
    interface::{IPCSharedEntity, LocalEntityIf, SharedEntityIf},
    vqueue::*,
};
use alloc::{
    collections::btree_map::BTreeMap,
    string::{String, ToString},
    vec::Vec,
};
use core::{
    ops::Deref,
    pin::Pin,
    task::{Context, Poll, Waker},
};
use kspin::SpinRaw;

pub struct QueueBasedSharedEntity {
    queue_id: usize,
}

impl SharedEntityIf for QueueBasedSharedEntity {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        self.queue_id as u64
    }

    fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized,
    {
        Ok(Self {
            queue_id: id as usize,
        })
    }

    /// 发送消息给self
    fn send_to(&self, item: IPCItem) -> Result<(), String> {
        push(self.queue_id, item).map_err(|_| "send failed".to_string())
    }
}

impl Drop for QueueBasedSharedEntity {
    fn drop(&mut self) {
        let _to_drop = QueueBasedSharedEntity::from_id(self.queue_id as u64);
    }
}

pub struct QueueBasedLocalEntity {
    shared: IPCSharedEntity,
    // slot_ref: SlotRef<'static, LockFreeDeque<IPCItem, QUEUE_CAPACITY>, ARRAY_LEN>,
    use_default_dispatcher: bool,
    wait_queue: SpinRaw<BTreeMap<u64, Vec<Waker>>>, // 只在同一进程的协程间同步，因此可以使用SpinRaw
    immediate_values: SpinRaw<BTreeMap<u64, (u64, [u64; 8])>>,
}

// 构造函数
impl QueueBasedLocalEntity {
    pub fn new(use_default_dispatcher: bool) -> Result<Self, String> {
        let queue = register_queue().map_err(|_| "register queue failed.".to_string())?;
        Ok(Self {
            shared: IPCSharedEntity::QueueBased(QueueBasedSharedEntity::from_id(
                // queue.clone().into_id() as u64,
                queue.into_id() as u64,
            )?),
            // slot_ref: queue,
            use_default_dispatcher,
            wait_queue: SpinRaw::new(BTreeMap::new()),
            immediate_values: SpinRaw::new(BTreeMap::new()),
        })
    }
}

impl LocalEntityIf for QueueBasedLocalEntity {
    async fn recv_inner(&'static self, msg_type: u64) -> Result<IPCItem, String> {
        WaitIPCFuture {
            entity: self,
            msg_type,
        }
        .await
    }
}

impl QueueBasedLocalEntity {
    /// 从self接收任意类型的消息，返回消息类型与消息内容。
    ///
    /// 返回值：OK((msg_type: u64, data: [u64; 8]))或Err(String)
    pub async fn recv_any(&self) -> Result<(u64, [u64; 8]), String> {
        if self.use_default_dispatcher {
            return Err("`recv_any` not supported with default dispatcher".to_string());
        }
        self.recv_any_inner()
            .await
            .map(|item| (item.msg_type, item.data))
    }

    pub async fn default_dispatcher(&self) -> ! {
        loop {
            // todo: 传递数据给对应的等待者
            if let Ok(item) = self.recv_any_inner().await {
                if let Some(waker_list) = self.wait_queue.lock().get_mut(&item.msg_type) {
                    if let Some(waker) = waker_list.pop() {
                        if self
                            .immediate_values
                            .lock()
                            .insert(item.msg_type, (item.sender, item.data))
                            .is_some()
                        {
                            #[cfg(feature = "log")]
                            log::warn!(
                                "Overwriting immediate value for msg_type {}",
                                item.msg_type
                            );
                        }
                        waker.wake();
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
            if let Some(item) = pop(queue_id) {
                return Ok(item);
            }
        }
    }
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

struct WaitIPCFuture {
    entity: &'static QueueBasedLocalEntity,
    msg_type: u64,
}

impl Future for WaitIPCFuture {
    type Output = Result<IPCItem, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some((sender, data)) = self.entity.immediate_values.lock().remove(&self.msg_type) {
            Poll::Ready(Ok(IPCItem {
                sender,
                msg_type: self.msg_type,
                data,
            }))
        } else {
            {
                let mut wait_queue = self.entity.wait_queue.lock();
                let waker_list = wait_queue.entry(self.msg_type).or_insert_with(Vec::new);
                waker_list.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}
