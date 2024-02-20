use std::collections::HashMap;

use polars::datatypes::TimeUnit;

#[derive(PartialEq, Debug, Clone)]
pub enum DataTypeDescriptor<'a> {
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

    Time(&'a str),
    /// parameter of Time() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Date(&'a str),
    /// parameter of Date() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Datetime(&'a str, TimeUnit, Option<String>),
    /// first parameter of Datetime() is format string according to
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    Categorical,
}

#[derive(PartialEq, Debug)]
pub struct CSVData<'a> {
    pub filename: String,
    pub separator: Option<String>,
    pub field_types: Option<HashMap<String, DataTypeDescriptor<'a>>>,
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
            field_types: None,
        }
    }
}
