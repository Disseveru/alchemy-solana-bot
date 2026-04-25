fn main() -> anyhow::Result<()> {
    use std::path::PathBuf;

    // Use the vendored protoc binary so a system installation of protoc is not
    // required.  This matches the approach used by yellowstone-grpc-proto.
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    // SAFETY: this build script is single-threaded and sets PROTOC before any
    // other code in the process can read or mutate the environment, so there is
    // no concurrent access to process environment state.
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    let proto_files = [
        PathBuf::from("protos/shared.proto"),
        PathBuf::from("protos/packet.proto"),
        PathBuf::from("protos/bundle.proto"),
        PathBuf::from("protos/searcher.proto"),
    ];
    let includes = [PathBuf::from("protos"), protoc_include];

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&proto_files, &includes)?;

    Ok(())
}
