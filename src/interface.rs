use core::ops::{Deref, DerefMut};

use crate::{
    queue_based::QueueBasedSharedEntity, sched_based::SchedBasedSharedEntity, vqueue::IPCItem,
};
use alloc::{format, string::String};

/// 本地进程持有的IPC实体。
///
/// 通过本类型的生命周期管理IPC实体的注册与注销。
pub trait LocalEntityIf: Deref<Target = IPCSharedEntity> {
    /// 从self向dst_id发送消息
    fn send(&self, dst_id: u64, msg_type: u64, data: [u64; 8]) -> Result<(), String> {
        let dst = IPCSharedEntity::from_id(dst_id)?;
        dst.send_to(IPCItem {
            sender: self.id(),
            msg_type,
            data,
        })
    }

    /// 从self接收msg_type类型的消息，返回消息内容
    async fn recv(&'static self, msg_type: u64) -> Result<[u64; 8], String> {
        let item = self.recv_inner(msg_type).await?;
        Ok(item.data)
    }

    /// 从self接收msg_type类型的消息
    async fn recv_inner(&'static self, msg_type: u64) -> Result<IPCItem, String>;
}

/// IPC实体中，可在进程间共享的部分。
///
/// 本类型的生命周期不应影响IPC实体的注册与注销，且本类型可能指向已注销的IPC实体。
pub trait SharedEntityIf {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64;

    /// 未增加类型识别符的id -> Self
    fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized;

    /// 发送消息给self
    fn send_to(&self, item: IPCItem) -> Result<(), String>;
}

pub enum IPCSharedEntity {
    QueueBased(QueueBasedSharedEntity),
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
    fn from_id(id: u64) -> Result<Self, String> {
        let high8 = id & 0xFF00_0000_0000_0000;
        match high8 {
            QUEUE_BASED_HIGH8 => {
                let ent = QueueBasedSharedEntity::from_id(id & 0x00FF_FFFF_FFFF_FFFF)?;
                Ok(IPCSharedEntity::QueueBased(ent))
            }
            SCHED_BASED_HIGH8 => {
                let ent = SchedBasedSharedEntity::from_id(id & 0x00FF_FFFF_FFFF_FFFF)?;
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
