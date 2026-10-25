fn main() {
    let config = slint_build::CompilerConfiguration::new().with_style("fluent-dark".into());
    slint_build::compile_with_config("ui/app.slint", config).unwrap();
    // Ícone do .exe. O da janela vem do `icon` em app.slint.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rerun-if-changed=icon.ico");
        embed_resource::compile("icon.rc", embed_resource::NONE).manifest_optional().unwrap();
    }
}