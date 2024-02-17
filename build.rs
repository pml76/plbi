
use cxx_qt_build::{CxxQtBuilder, QmlModule};


fn main() {
    // lalrpop::process_root().unwrap();

    let _ = lalrpop::Configuration::new()
        .emit_rerun_directives(true)
        .force_build(true)
        .process_current_dir();
    
    CxxQtBuilder::new()
        // Link Qt's Network library
        // - Qt Core is always linked
        // - Qt Gui is linked by enabling the qt_gui Cargo feature (default).
        // - Qt Qml is linked by enabling the qt_qml Cargo feature (default).
        // - Qt Qml requires linking Qt Network on macOS
        .qt_module("Network")
        .qml_module(QmlModule {
            uri: "com.kdab.cxx_qt.demo",
            rust_files: &["src/cxxqt_object.rs"],
            qml_files: &["qml/main.qml"],
            ..Default::default()
        })
        .build();
}
