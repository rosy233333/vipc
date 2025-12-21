use crate::vqueue::IPCItem;
use crate::{queue_based::QueueBasedEntity, sched_based::SchedBasedEntity};
use alloc::{format, string::String};

/// 本地IPC实体：创建时注册，释放时注销。
/// 通过id传递给其它进程。
struct Local<T: AbsIPCEntity>(T);

impl<T: AbsIPCEntity> Drop for Local<T> {
    fn drop(&mut self) {
        self.0.unregister().unwrap();
    }
}

impl<T: AbsIPCEntity> Local<T> {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        self.0.id()
    }

    fn send(&self, dst_id: u64, msg_type: u64, data: [u64; 8]) -> Result<(), String> {
        self.0.send(dst_id, msg_type, data)
    }

    async fn recv(&self, msg_type: u64) -> Result<[u64; 8], String> {
        self.0.recv(msg_type).await
    }

    async fn call(
        &self,
        dst_id: u64,
        send_type: u64,
        data: [u64; 8],
        recv_type: u64,
    ) -> Result<[u64; 8], String> {
        self.0.call(dst_id, send_type, data, recv_type).await
    }
}

/// 远程IPC实体：通过id获取，释放时不注销。
/// 可能指向已注销的实体，此时操作会失败。
struct Remote<T: AbsIPCEntity>(T);

impl<T: AbsIPCEntity> Remote<T> {
    fn from_id(id: u64) -> Result<Self, String> {
        T::from_id(id).map(|s| Self(s))
    }

    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        self.0.id()
    }

    // fn send(&self, dst_id: u64, msg_type: u64, data: [u64; 8]) -> Result<(), String> {
    //     self.0.send(dst_id, msg_type, data)
    // }

    // async fn recv(&self, dst_id: u64, msg_type: u64) -> Result<[u64; 8], String> {
    //     self.0.recv(dst_id, msg_type).await
    // }

    // async fn call(
    //     &self,
    //     dst_id: u64,
    //     send_type: u64,
    //     data: [u64; 8],
    //     recv_type: u64,
    // ) -> Result<[u64; 8], String> {
    //     self.0.call(dst_id, send_type, data, recv_type).await
    // }
}

pub enum IPCEntity {
    QueueBased(QueueBasedEntity),
    SchedBased(SchedBasedEntity),
}

const QUEUE_BASED_HIGH8: u64 = 0x01 << 56;
const SCHED_BASED_HIGH8: u64 = 0x02 << 56;

impl IPCEntity {
    pub fn id(&self) -> u64 {
        match self {
            IPCEntity::QueueBased(ent) => QUEUE_BASED_HIGH8 | ent.id(),
            IPCEntity::SchedBased(ent) => SCHED_BASED_HIGH8 | ent.id(),
        }
    }

    pub fn from_id(id: u64) -> Result<Self, String> {
        let high8 = id & 0xFF00_0000_0000_0000;
        match high8 {
            QUEUE_BASED_HIGH8 => {
                let ent = QueueBasedEntity::from_id(id & 0x00FF_FFFF_FFFF_FFFF)?;
                Ok(IPCEntity::QueueBased(ent))
            }
            SCHED_BASED_HIGH8 => {
                let ent = SchedBasedEntity::from_id(id & 0x00FF_FFFF_FFFF_FFFF)?;
                Ok(IPCEntity::SchedBased(ent))
            }
            _ => Err(format!("Unknown IPC entity type with id: {}", id)),
        }
    }
}

impl AbsIPCEntity for IPCEntity {
    fn unregister(&mut self) -> Result<(), String> {
        match self {
            IPCEntity::QueueBased(ent) => ent.unregister(),
            IPCEntity::SchedBased(ent) => ent.unregister(),
        }
    }

    fn id(&self) -> u64 {
        self.id()
    }

    fn from_id(id: u64) -> Result<Self, String> {
        Self::from_id(id)
    }

    fn send_to_inner(&self, item: IPCItem) -> Result<(), String> {
        match self {
            IPCEntity::QueueBased(ent) => ent.send_to_inner(item),
            IPCEntity::SchedBased(ent) => ent.send_to_inner(item),
        }
    }

    async fn recv_inner(&self, msg_type: u64) -> Result<IPCItem, String> {
        match self {
            IPCEntity::QueueBased(ent) => ent.recv_inner(msg_type).await,
            IPCEntity::SchedBased(ent) => ent.recv_inner(msg_type).await,
        }
    }
}

pub(crate) trait AbsIPCEntity: Sized {
    // 因为rigister可能需要提供不同参数，因此不统一定义
    // fn register() -> Result<Self, String>;

    /// 在本函数中注销IPC实体。若为本地实体，则该函数会自动在`drop`中调用。在远程实体中不会调用。
    ///
    /// 若返回Err，则会在drop中panic。
    ///
    /// 应该将注销逻辑放在这里，而不是drop中，以防止释放远程实体导致注销。
    fn unregister(&mut self) -> Result<(), String>;

    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64;
    fn from_id(id: u64) -> Result<Self, String>;

    fn send(&self, dst_id: u64, msg_type: u64, data: [u64; 8]) -> Result<(), String> {
        let dst = Remote::<IPCEntity>::from_id(dst_id)?;
        dst.0.send_to_inner(IPCItem {
            sender: self.id(),
            msg_type,
            data,
        })
    }

    async fn recv(&self, msg_type: u64) -> Result<[u64; 8], String> {
        let item = self.recv_inner(msg_type).await?;
        Ok(item.data)
    }

    async fn call(
        &self,
        dst_id: u64,
        send_type: u64,
        data: [u64; 8],
        recv_type: u64,
    ) -> Result<[u64; 8], String> {
        self.send(dst_id, send_type, data)?;
        self.recv(recv_type).await
    }

    fn send_to_inner(&self, item: IPCItem) -> Result<(), String>;
    async fn recv_inner(&self, msg_type: u64) -> Result<IPCItem, String>;
}
