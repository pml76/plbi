use std::string::String;

pub enum PlbiError {
    PlbiFileNotfound(String),
    PlbiCouldNotReadFile(String),
    PlbiFileNameWithoutStem(String),
}
