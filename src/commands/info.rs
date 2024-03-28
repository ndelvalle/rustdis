use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::commands::executable::Executable;
use crate::commands::CommandParser;
use crate::frame::Frame;
use crate::store::Store;
use crate::Error;

const INFO: &str = r#"
# Server
redis_version:7.2.4
os:Linux 5.15.0-1015-aws x86_64
arch_bits:64
process_id:1
uptime_in_seconds:1030110
tcp_port:6379

# Clients
connected_clients:1
maxclients:10000

# Memory
used_memory:68824640
used_memory_human:65.64M
used_memory_peak:68848456
used_memory_peak_human:65.66M
maxmemory:4294967296
maxmemory_human:4.00G

# Persistence
loading:0
rdb_changes_since_last_save:1050288
aof_enabled:0

# Stats
total_connections_received:21
total_commands_processed:1308336
instantaneous_ops_per_sec:0

# Replication
role:master
connected_slaves:0

# CPU
used_cpu_sys:850.545934
used_cpu_user:1777.532734

# Errorstats
errorstat_ERR:count:1189

# Cluster
cluster_enabled:0

# Keyspace
db0:keys=397255,expires=845,avg_ttl=1527956522210785
"#;

#[derive(Debug, PartialEq)]
pub struct Info;

impl Executable for Info {
    fn exec(self, _store: Arc<Mutex<Store>>) -> Result<Frame, Error> {
        Ok(Frame::Bulk(Bytes::from(INFO)))
    }
}

impl TryFrom<&mut CommandParser> for Info {
    type Error = Error;

    fn try_from(_parser: &mut CommandParser) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}
