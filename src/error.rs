use std::string::String;

pub enum TldrError {
    TldrFileNotfound(String),
    TldrCouldNotReadFile(String),
    TldrFileNameWithoutStem(String),
}
