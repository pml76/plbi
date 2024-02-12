use std::string::String;

#[derive(PartialEq, Debug)]
pub struct CSVData {
    pub filename: String,
    pub separator: Option<String>,
}

#[derive(PartialEq, Debug)]
pub enum LoadableFormatData {
    CSV(CSVData),
}

#[derive(PartialEq, Debug)]
pub struct Ast {
    pub loadable_filenames: Vec<LoadableFormatData>,
}

impl CSVData {
    fn new(filename: String, separator: Option<String>) -> CSVData {
        CSVData {
            filename,
            separator,
        }
    }
}
