use std::string::String;



pub enum PlbiError {
    PlbiFileNotfound,
    PlbiCouldNotReadFile(String)
}