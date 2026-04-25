fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_files = [
        "protos/jito/searcher.proto",
        "protos/jito/bundle.proto",
        "protos/jito/packet.proto",
        "protos/jito/shared.proto",
    ];

    for proto in proto_files {
        println!("cargo:rerun-if-changed={proto}");
    }

    tonic_build::configure()
        .build_server(false)
        .compile(&proto_files, &["protos/jito"])?;

    Ok(())
}
