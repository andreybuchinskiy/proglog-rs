fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.message_attribute(
        "log.v1.Record",
        "#[derive(serde::Serialize, serde::Deserialize)]",
    );
    config.compile_protos(&["src/api/v1/log.proto"], &["src/api/"])?;
    Ok(())
}
