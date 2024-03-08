use std::string::String;

#[derive(Debug)]
pub enum TldrError {
    TldrFileNotfound(String),
    TldrCouldNotReadFile(String),
    TldrFileNameWithoutStem(String),
    TldrCouldNotReadSchema(String),
    TldrCouldNotMergeSchemas(String),
    TldrCouldNotCreateMemTable(String),
    TldrCouldNotRegisterTable(String),
}
