use std::string::String;

#[derive(PartialEq, Debug)]
pub struct Ast {
    pub loadable_filenames: Vec<String>,
}
