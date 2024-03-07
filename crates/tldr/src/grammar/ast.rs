use arrow::datatypes::TimeUnit;
use std::collections::HashMap;

#[derive(PartialEq, Debug, Clone)]
pub enum DataTypeDescriptor<'a> {
    UInt8(bool),
    UInt16(bool),
    UInt32(bool),
    UInt64(bool),
    Int8(bool),
    Int16(bool),
    Int32(bool),
    Int64(bool),
    Float32(bool),
    Float64(bool),
    Null,
    Boolean(bool),
    Binary(bool),
    String(bool),
    Duration(bool, TimeUnit),

    Time(bool, &'a str),
    /// parameter of Time() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Date(bool, &'a str),
    /// parameter of Date() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Datetime(bool, &'a str, TimeUnit, Option<String>),
}

#[derive(PartialEq, Debug)]
pub struct CSVData<'a> {
    pub filename: String,
    pub separator: Option<String>,
    pub field_types: HashMap<String, DataTypeDescriptor<'a>>,
}

#[derive(PartialEq, Debug)]
pub enum LoadableFormatData<'a> {
    CSV(CSVData<'a>),
}

#[derive(PartialEq, Debug)]
pub struct Ast<'a> {
    pub loadable_filenames: Vec<LoadableFormatData<'a>>,
}

impl<'a> CSVData<'a> {
    pub fn new(filename: String, separator: Option<String>) -> CSVData<'a> {
        CSVData {
            filename,
            separator,
            field_types: HashMap::new(),
        }
    }
}
