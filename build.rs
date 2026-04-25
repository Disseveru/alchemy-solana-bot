fn main() -> anyhow::Result<()> {
    // Use the vendored protoc binary so a system installation of protoc is not
    // required.  This matches the approach used by yellowstone-grpc-proto.
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: setting an environment variable is safe in a single-threaded
    // build script that runs before any other threads are spawned.
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "protos/shared.proto",
                "protos/packet.proto",
                "protos/bundle.proto",
                "protos/searcher.proto",
            ],
            &["protos"],
        )?;

    Ok(())
}
