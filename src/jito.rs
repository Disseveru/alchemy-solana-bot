//! Generated Jito gRPC types and client stubs.
//!
//! These modules are populated at build time by `build.rs` using
//! `tonic-prost-build` + the proto files in `protos/`.  Do **not** edit the
//! contents of these modules by hand.

/// Shared header type used across Jito gRPC messages.
pub mod shared {
    tonic::include_proto!("shared");
}

/// Low-level packet type that wraps a serialized transaction.
pub mod packet {
    tonic::include_proto!("packet");
}

/// Bundle type (a group of packets submitted atomically).
pub mod bundle {
    tonic::include_proto!("bundle");
}

/// Jito Block Engine Searcher Service client and request/response types.
pub mod searcher {
    tonic::include_proto!("searcher");
}
