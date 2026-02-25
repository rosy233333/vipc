//! 定义了不同通信方式的IPC entity采用的统一接口[`LocalEntityIf`]和[`SharedEntityIf`]，
//! 以及包装了不同类型`impl SharedEntityIf`的统一共享IPC entity类型[`IPCSharedEntity`]

use core::ops::{Deref, DerefMut};

use crate::{
    queue_based::QueueBasedSharedEntity, sched_based::SchedBasedSharedEntity, vqueue::IPCItem,
};
use alloc::{format, string::String};
// use futures::stream::Stream;

/// 本地进程持有的IPC实体。
///
/// ## 需实现的接口
///
/// - [`LocalEntityIf::recv`]：接收消息。
/// - `impl Deref<Target = IPCSharedEntity>`：每个LocalEntity需要包含一个IPCSharedEntity，并可解引用到它。
/// - 生命周期管理：通过本类型的生命周期管理IPC实体的注册与注销。
///
/// ## 提供的接口
///
/// - [`LocalEntityIf::send`]：发送消息。
/// - [`LocalEntityIf::call`]：发送消息并等待返回值。
pub trait LocalEntityIf: Deref<Target = IPCSharedEntity> {
    /// 从self向dst_id发送消息
    fn send(
        &self,
        dst_id: u64,
        msg_type: u64,
        rep_type: u64,
        data: [u64; 8],
    ) -> Result<(), String> {
        // #[cfg(feature = "log")]
        // log::debug!("send: before getting dst, dst_id: 0x{:016x}", dst_id);
        let dst = unsafe { IPCSharedEntity::from_id(dst_id)? };
        // #[cfg(feature = "log")]
        // log::debug!("send to id: {:#x}", dst.id());
        let res = dst.send_to(IPCItem {
            sender: self.id(),
            msg_type,
            rep_type,
            data,
        });
        // #[cfg(feature = "log")]
        // log::debug!("send result: {:?}", res);
        res
    }

    /// 从self接收msg_type类型的消息
    ///
    /// 需要具备缓存消息与通知的功能。
    /// 即：在循环中调用`recv`时，不会丢失两次调用之间到达的消息。
    /// 也不会因为通知机制（如信号）在两次调用之间到达而无法唤醒。
    async fn recv(&'static self, msg_type: u64) -> Result<IPCItem, String>;

    // /// 从self持续接收msg_type类型的消息。
    // ///
    // /// 该函数会返回一个Stream（流/异步迭代器）。
    // ///
    // /// 使用单独接口的原因时在循环中调用`recv`可能会丢失两次调用之间到达的消息，但`recv_stream`则不会丢失。
    // fn recv_stream(&'static self, msg_type: u64) -> impl Stream<Item = Result<IPCItem, String>>;

    /// 从self向dst_id发送消息，并等待回复
    async fn call(
        &'static self,
        dst_id: u64,
        msg_type: u64,
        rep_type: u64,
        data: [u64; 8],
    ) -> Result<IPCItem, String> {
        // #[cfg(feature = "log")]
        // log::debug!("call: before send");
        self.send(dst_id, msg_type, rep_type, data)?;
        // #[cfg(feature = "log")]
        // log::debug!("call: after send");
        let res = self.recv(rep_type).await;
        // #[cfg(feature = "log")]
        // log::debug!("call: after recv");
        res
    }
}

/// IPC实体中，可在进程间共享的部分。
///
/// ## 需实现的接口
///
/// - [`SharedEntityIf::id`]、[`SharedEntityIf::from_id`]：共享实体需可与id相互转换。
/// - [`SharedEntityIf::send_to`]：外部向自己发送消息。
/// - 生命周期管理：本类型的生命周期不应影响IPC实体的注册与注销，且本类型可能指向已注销的IPC实体。
pub trait SharedEntityIf {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64;

    /// 未增加类型识别符的id -> Self
    ///
    /// # Safety
    ///
    /// 调用者需确保id参数由本类型对象的`id`方法获得。
    unsafe fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized;

    /// 发送消息给self
    fn send_to(&self, item: IPCItem) -> Result<(), String>;
}

/// 包装了不同类型`impl SharedEntityIf`的统一共享IPC entity类型。
///
/// 使用此类型，目的是在id中加入类型识别符，从而可以将各个类型的共享实体与id相互转化，且不冲突。
///
/// 该类型在两个地方被使用：
///
/// 1. LocalEntity中：每个LocalEntity包含一个IPCSharedEntity，并可解引用到它，从而实现LocalEntityIf的接口。
/// 2. 在进程间传递共享实体的id时，均使用IPCSharedEntity的id接口，且在从id转回实体时，使用IPCSharedEntity的from_id接口，从而实现不同类型的共享实体与id的相互转换。
pub enum IPCSharedEntity {
    /// 基于队列的IPC实体，详见`queue_based.rs`
    QueueBased(QueueBasedSharedEntity),
    /// IPC队列与调度队列统一的IPC实体，详见`sched_based.rs`
    SchedBased(SchedBasedSharedEntity),
}

const QUEUE_BASED_HIGH8: u64 = 0x01 << 56;
const SCHED_BASED_HIGH8: u64 = 0x02 << 56;

impl SharedEntityIf for IPCSharedEntity {
    /// 已增加了类型识别符的id
    fn id(&self) -> u64 {
        match self {
            IPCSharedEntity::QueueBased(ent) => QUEUE_BASED_HIGH8 | ent.id(),
            IPCSharedEntity::SchedBased(ent) => SCHED_BASED_HIGH8 | ent.id(),
        }
    }

    /// 已增加类型识别符的id -> Self
    unsafe fn from_id(id: u64) -> Result<Self, String> {
        let high8 = id & 0xFF00_0000_0000_0000;
        match high8 {
            QUEUE_BASED_HIGH8 => {
                let ent = unsafe { QueueBasedSharedEntity::from_id(id & 0x00FF_FFFF_FFFF_FFFF) }?;
                Ok(IPCSharedEntity::QueueBased(ent))
            }
            SCHED_BASED_HIGH8 => {
                let ent = unsafe { SchedBasedSharedEntity::from_id(id & 0x00FF_FFFF_FFFF_FFFF) }?;
                Ok(IPCSharedEntity::SchedBased(ent))
            }
            _ => Err(format!("Unknown IPC entity type with id: {}", id)),
        }
    }

    fn send_to(&self, item: IPCItem) -> Result<(), String> {
        match self {
            IPCSharedEntity::QueueBased(ent) => ent.send_to(item),
            IPCSharedEntity::SchedBased(ent) => ent.send_to(item),
        }
    }
}
