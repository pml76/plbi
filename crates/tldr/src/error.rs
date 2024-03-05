use std::string::String;

#[derive(Debug)]
pub enum TldrError {
    TldrFileNotfound(String),
    TldrCouldNotReadFile(String),
    TldrFileNameWithoutStem(String),
}
