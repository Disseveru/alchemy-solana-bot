fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to fetch vendored protoc");
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .compile(
            &[
                "protos/auth.proto",
                "protos/block.proto",
                "protos/block_engine.proto",
                "protos/bundle.proto",
                "protos/packet.proto",
                "protos/relayer.proto",
                "protos/searcher.proto",
                "protos/shared.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
