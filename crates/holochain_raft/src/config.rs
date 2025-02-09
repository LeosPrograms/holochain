use std::io::Cursor;

openraft::declare_raft_types!(
   pub TypeConfig:
       NodeId       = u64,
);
