pub mod context;
pub mod cxxqt_object;
pub mod error;
pub mod grammar;

use cxx_qt_lib::{QGuiApplication, QQmlApplicationEngine, QUrl};
pub fn main2() {
    // Create the application and engine
    let mut app = QGuiApplication::new();
    let mut engine = QQmlApplicationEngine::new();

    // Load the QML path into the engine
    if let Some(engine) = engine.as_mut() {
        engine.load(&QUrl::from("qrc:/qt/qml/com/kdab/cxx_qt/demo/qml/main.qml"));
    }

    // Start the app
    if let Some(app) = app.as_mut() {
        app.exec();
    }
}

