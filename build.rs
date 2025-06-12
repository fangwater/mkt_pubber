use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 生成 Cap'n Proto 代码
    capnpc::CompilerCommand::new()
        .file("period.capnp")
        .run()
        .expect("compiling capnp schema");

    // 生成 Protocol Buffers 代码
    tonic_build::compile_protos("period.proto")?;
    
    Ok(())
} 