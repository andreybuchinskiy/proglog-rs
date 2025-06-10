fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.type_attribute(".", "#[serde(rename_all = \"snake_case\")]");
    config.message_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos_with_config(config, &["src/api/v1/log.proto"], &["src/api/v1"])?;
    Ok(())
}
