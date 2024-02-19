use std::collections::HashMap;

use polars::datatypes::{DataType, TimeUnit};

#[derive(PartialEq, Debug, Clone)]
pub enum DataTypeDescriptor {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Null,
    Unknown,
    Boolean,
    Binary,
    BinaryOffset,
    String,
    Duration(TimeUnit),

    Time(String),
    /// parameter of Time() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Date(String),
    /// parameter of Date() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Datetime(String, TimeUnit, Option<String>),
    /// first parameter of Datetime() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Categorical,
}

#[derive(PartialEq, Debug)]
pub struct CSVData {
    pub filename: String,
    pub separator: Option<String>,
    pub field_types: Option<HashMap<String, DataTypeDescriptor>>,
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
    pub fn new(filename: String, separator: Option<String>) -> CSVData {
        CSVData {
            filename,
            separator,
            field_types: None,
        }
    }
}
