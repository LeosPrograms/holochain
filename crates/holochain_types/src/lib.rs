//! Common types used by other Holochain crates.
//!
//! This crate is a complement to the
//! [holochain_zome_types crate](https://crates.io/crates/holochain_zome_types),
//! which contains only the essential types which are used in Holochain DNA
//! code. This crate expands on those types to include all types which Holochain
//! itself depends on.

#![deny(missing_docs)]
// We have a lot of usages of type aliases to `&String`, which clippy objects to.
#![allow(clippy::ptr_arg)]
// TODO - address the underlying issue:
#![allow(clippy::result_large_err)]
#![allow(non_local_definitions)]

pub mod access;
pub mod action;
pub mod activity;
pub mod app;
pub mod autonomic;
pub mod chain;
pub mod combinators;
pub mod countersigning;
pub mod db;
pub mod db_cache;
pub mod dht_op;
pub mod dna;
pub mod entry;
pub mod link;
mod macros;
pub mod metadata;
pub mod prelude;
pub mod rate_limit;
pub mod record;
pub mod share;
#[warn(missing_docs)]
pub mod sql;
pub mod validation_receipt;
pub mod warrant;
pub mod web_app;
pub mod zome_types;

#[cfg(feature = "fixturators")]
pub mod fixt;

#[cfg(feature = "test_utils")]
pub mod inline_zome;
#[cfg(feature = "test_utils")]
pub mod test_utils;
pub mod websocket;

use holochain_keystore::{AgentPubKeyExt, LairResult, MetaLairClient};
pub use holochain_zome_types::entry::EntryHashed;
use holochain_zome_types::{
    prelude::Signature,
    zome_io::{ExternIO, ZomeCallParams},
};

/// Convert to the older deepkey version of an HDK prelude type
#[macro_export]
macro_rules! deepkey_roundtrip_backward(
    ($t:ident, $v:expr) => {{
        let v: &$t = $v;
        let v: hc_deepkey_sdk::hdk::prelude::$t = holochain_serialized_bytes::decode(
            &holochain_serialized_bytes::encode(v).expect("Couldn't roundtrip encode"),
        )
        .expect("Couldn't roundtrip decode");
        v
    }}
);

/// Convert from the older deepkey version of an HDK prelude type
#[macro_export]
macro_rules! deepkey_roundtrip_forward(
    ($t:ident, $v:expr) => {{
        let v: &hc_deepkey_sdk::hdk::prelude::$t = $v;
        let v: $t = holochain_serialized_bytes::decode(
            &holochain_serialized_bytes::encode($v).expect("Couldn't roundtrip encode"),
        )
        .expect("Couldn't roundtrip decode");
        v
    }}
);

/// The data provided over an app interface in order to make a zome call.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ZomeCallParamsSigned {
    /// Bytes of the serialized zome call payload that consists of all fields of the
    /// [`ZomeCallParams`].
    pub bytes: ExternIO,
    /// Signature by the provenance of the call, signing the bytes of the zome call payload.
    pub signature: Signature,
}

impl ZomeCallParamsSigned {
    /// Constructor
    pub fn new(bytes: Vec<u8>, signature: Signature) -> Self {
        Self {
            bytes: ExternIO::from(bytes),
            signature,
        }
    }

    /// Try to construct a [`ZomeCallParamsSigned`] from a [`ZomeCallParams`] and a keystore.
    pub async fn try_from_params(
        keystore: &MetaLairClient,
        params: ZomeCallParams,
    ) -> LairResult<Self> {
        let (bytes, bytes_hash) = params.serialize_and_hash().map_err(|e| e.to_string())?;
        let signature = params
            .provenance
            .sign_raw(keystore, bytes_hash.into())
            .await?;
        Ok(Self::new(bytes, signature))
    }
}
