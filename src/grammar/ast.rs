use polars::datatypes;
use std::string::String::DataType;

#[derive(PartialEq, Debug)]
pub struct CSVData {
    pub filename: String,
    pub separator: Option<String>,
    pub field_types: Option<HashMap<String, DataType>>,
}

#[derive(PartialEq, Debug)]
pub enum LoadableFormatData {
    CSV(CSVData),
}

#[derive(PartialEq, Debug)]
pub struct Ast {
    pub loadable_filenames: Vec<LoadableFormatData>,
}

pub enum DataType {}

impl CSVData {
    fn new(filename: String, separator: Option<String>) -> CSVData {
        CSVData {
            filename,
            separator,
        }
    }
}
