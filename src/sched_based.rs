//! 与调度器整合的IPC实体
//!
//! 还未实现

use core::{
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

// use crate::interface::AbsIPCEntity;
use crate::{
    interface::{IPCSharedEntity, LocalEntityIf, SharedEntityIf},
    vqueue::IPCItem,
};
use alloc::{
    collections::{btree_map::BTreeMap, vec_deque::VecDeque},
    string::{String, ToString},
};
use kspin::SpinRaw;
use libvqueue::{
    deque_is_empty, deque_pop, deque_push, get_pid, register_process, slotref_from_id,
};
use libvsched2::schedule::event_source::EventSource;

/// 与调度器整合的共享IPC实体（共享实体与本地实体的关系见`interface.rs`）
///
/// 每个该对象持有一个SlotRef的引用计数。
pub struct SchedBasedSharedEntity {
    queue_id: usize,
}

impl SharedEntityIf for SchedBasedSharedEntity {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        self.queue_id as u64
    }

    unsafe fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized,
    {
        // #[cfg(feature = "log")]
        // log::debug!("SchedBasedSharedEntity::from_id: id=0x{:016x}", id);
        // 增加引用计数
        let slot_ref = unsafe { slotref_from_id(id as usize) };
        // #[cfg(feature = "log")]
        // log::debug!(
        //     "SchedBasedSharedEntity::from_id: slot_ref={:?}, rc={} before increase",
        //     slot_ref,
        //     slot_ref.rc()
        // );
        slot_ref.clone().into_id();
        // #[cfg(feature = "log")]
        // log::debug!(
        //     "SchedBasedSharedEntity::from_id: slot_ref={:?}, rc={} after increase",
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
        // #[cfg(feature = "log")]
        // log::debug!(
        //     "SchedBasedSharedEntity::send_to: queue_id={}",
        //     self.queue_id
        // );
        let res = deque_push(self.queue_id, item).map_err(|_| "send failed".to_string());
        let pid = get_pid(self.queue_id) as u64;
        // #[cfg(feature = "log")]
        // log::debug!("SchedBasedSharedEntity::send_to result: {:?}", res);
        res
    }
}

impl Drop for SchedBasedSharedEntity {
    fn drop(&mut self) {
        let to_drop = unsafe { slotref_from_id(self.queue_id) };
        // #[cfg(feature = "log")]
        // log::debug!(
        //     "SchedBasedSharedEntity::drop: slot_ref={:?}, rc={} before decrease",
        //     to_drop,
        //     to_drop.rc()
        // );
    }
}

/// 与调度器整合的本地IPC实体（共享实体与本地实体的关系见`interface.rs`）
///
/// ## 消息接收机制
///
/// ### `recv_any = true`时（请求队列）
///
/// 调用`recv_any`的协程直接从队列中获取消息，未获取到则阻塞并等待事件源机制唤醒。
///
/// ### `recv_any = false`时（响应队列）
///
/// 在接收消息时，首先从暂存区`immediate_values`中获取，未获取到则阻塞并等待事件源机制唤醒。
///
/// ## 注意事项
///
/// 调用`recv`时的`msg_type`参数，以及调用`call`时的`rep_type`参数都需要设置为当前任务的指针。
pub struct SchedBasedLocalEntity {
    shared: IPCSharedEntity,
    /// - true: 通过同一协程接收不同类型的消息。`recv`不可用，`recv_any`可用
    /// - false：不同协程接收不同类型的消息。`recv`可用，`recv_any`不可用
    recv_any: bool,
    /// 只在`recv_any = true`时使用，存储阻塞的消息处理任务。
    ///
    /// （`recv_any = false`时，任务指针从消息的`msg_type`中获取）
    wait_queue: SpinRaw<VecDeque<usize>>,
    /// key: msg_type,
    ///
    /// value: (sender, rep_type, data)
    immediate_values: SpinRaw<BTreeMap<u64, (u64, u64, [u64; 8])>>,
    /// 当队列中有消息且有正在阻塞的协程时，该事件源的优先级。
    ///
    /// 应该设置为接收消息的协程的优先级。如果有多个接收消息的协程，则应保持它们优先级相同。
    active_prio: isize,
    /// 当队列中没有消息时，该事件源的优先级。
    ///
    /// 应该设置为比协程最低优先级更低一级（即数值高1）的优先级。
    inactive_prio: isize,
}

impl SchedBasedLocalEntity {
    /// 构造函数
    ///
    /// 参数：
    ///
    /// - `recv_any`:
    ///     - true: 通过同一协程接收不同类型的消息。`recv`不可用，`recv_any`可用
    ///     - false：不同协程接收不同类型的消息。`recv`可用，`recv_any`不可用
    /// - `active_prio`: 当队列中有消息时，该事件源的优先级。
    ///     - 应该设置为接收消息的协程的优先级。如果有多个接收消息的协程，则应保持它们优先级相同。
    /// - `inactive_prio`: 当队列中没有消息时，该事件源的优先级。
    ///     - 应该设置为比协程最低优先级更低一级（即数值高1）的优先级。
    pub fn new(recv_any: bool, active_prio: isize, inactive_prio: isize) -> Result<Self, String> {
        let queue = register_process().map_err(|_| "register queue failed.".to_string())?;
        // log::debug!(
        //     "SchedBasedLocalEntity::new: slot_ref={:?}, rc={} after increase",
        //     queue,
        //     queue.rc()
        // );

        let queue_id = queue.into_id();
        Ok(Self {
            shared: IPCSharedEntity::SchedBased(SchedBasedSharedEntity { queue_id }),
            recv_any,
            wait_queue: SpinRaw::new(VecDeque::new()),
            immediate_values: SpinRaw::new(BTreeMap::new()),
            active_prio,
            inactive_prio,
        })
    }
}

impl LocalEntityIf for SchedBasedLocalEntity {
    /// 只能在`recv_any = false`时使用，且`msg_type`必须为当前任务的指针。
    async fn recv(&'static self, msg_type: u64) -> Result<IPCItem, String> {
        if self.recv_any {
            panic!("`recv` can only be used with `recv_any = false`!");
        }
        wait_recv(self, msg_type).await
    }
}

impl Deref for SchedBasedLocalEntity {
    type Target = IPCSharedEntity;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

/// 从`immediate_values`中获取IPC消息，若获取不到则等待事件源将消息放入`immediate_values`后唤醒。
///
/// 因为使用事件源的唤醒机制，因此不再需要注册Waker。
struct WaitRecvFuture {
    entity: &'static SchedBasedLocalEntity,
    msg_type: u64,
}

fn wait_recv(entity: &'static SchedBasedLocalEntity, msg_type: u64) -> WaitRecvFuture {
    WaitRecvFuture { entity, msg_type }
}

impl Future for WaitRecvFuture {
    type Output = Result<IPCItem, String>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
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
            // log::debug!("WaitDispatchFuture: return Pending");
            Poll::Pending
        }
    }
}

impl SchedBasedLocalEntity {
    /// 只能在`recv_any = true`时使用。
    pub async fn recv_any(&'static self, current_task: *const ()) -> Result<IPCItem, String> {
        if !self.recv_any {
            panic!("`recv_any` can only be used with `recv_any = true`!");
        }
        wait_recv_any(self, current_task as usize).await
    }
}

/// 从队列中获取IPC消息，若获取不到则等待消息进入队列后，由事件源唤醒。
///
/// 因为使用事件源的唤醒机制，因此不再需要注册Waker。
struct WaitRecvAnyFuture {
    entity: &'static SchedBasedLocalEntity,
    current_task: usize,
}

fn wait_recv_any(entity: &'static SchedBasedLocalEntity, current_task: usize) -> WaitRecvAnyFuture {
    WaitRecvAnyFuture {
        entity,
        current_task,
    }
}

impl Future for WaitRecvAnyFuture {
    type Output = Result<IPCItem, String>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let queue_id = match &self.entity.shared {
            IPCSharedEntity::SchedBased(ent) => ent.queue_id,
            _ => panic!("invalid shared entity type"),
        };
        if let Some(item) = deque_pop(queue_id) {
            // #[cfg(feature = "log")]
            // log::debug!("WaitDispatchFuture: return Ready");
            Poll::Ready(Ok(item))
        } else {
            // #[cfg(feature = "log")]
            // log::debug!("WaitDispatchFuture: return Pending");
            self.entity.wait_queue.lock().push_back(self.current_task);
            Poll::Pending
        }
    }
}

// 实现事件源
impl EventSource for SchedBasedLocalEntity {
    fn hightest_priority(&self, _cpu_id: usize) -> isize {
        let queue_id = match &self.shared {
            IPCSharedEntity::SchedBased(ent) => ent.queue_id,
            _ => panic!("invalid shared entity type"),
        };
        if self.recv_any {
            // 请求队列
            if deque_is_empty(queue_id) || self.wait_queue.lock().is_empty() {
                self.inactive_prio
            } else {
                self.active_prio
            }
        } else {
            // 响应队列
            if deque_is_empty(queue_id) {
                self.inactive_prio
            } else {
                self.active_prio
            }
        }
    }

    fn take_task(&self, _cpu_id: usize) -> (*const (), isize) {
        let queue_id = match &self.shared {
            IPCSharedEntity::SchedBased(ent) => ent.queue_id,
            _ => panic!("invalid shared entity type"),
        };
        if self.recv_any {
            // 请求队列
            if deque_is_empty(queue_id) {
                (core::ptr::null(), self.inactive_prio)
            } else {
                if let Some(task) = self.wait_queue.lock().pop_front() {
                    if self.wait_queue.lock().is_empty() {
                        (task as *const (), self.inactive_prio)
                    } else {
                        (task as *const (), self.active_prio)
                    }
                } else {
                    (core::ptr::null(), self.inactive_prio)
                }
            }
        } else {
            // 响应队列
            if let Some(item) = deque_pop(queue_id) {
                let task = item.msg_type as usize;
                if self
                    .immediate_values
                    .lock()
                    .insert(item.msg_type, (item.sender, item.rep_type, item.data))
                    .is_some()
                {
                    #[cfg(feature = "log")]
                    log::warn!("Overwriting immediate value for msg_type {}", item.msg_type);
                }
                if deque_is_empty(queue_id) {
                    (task as *const (), self.inactive_prio)
                } else {
                    (task as *const (), self.active_prio)
                }
            } else {
                (core::ptr::null(), self.inactive_prio)
            }
        }
    }
}
