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

impl<'a> DataTypeDescriptor<'a> {
    pub fn is_nullable(&self) -> bool {
        match self {
            DataTypeDescriptor::UInt8(b) => *b,
            DataTypeDescriptor::UInt16(b) => *b,
            DataTypeDescriptor::UInt32(b) => *b,
            DataTypeDescriptor::UInt64(b) => *b,
            DataTypeDescriptor::Int8(b) => *b,
            DataTypeDescriptor::Int16(b) => *b,
            DataTypeDescriptor::Int32(b) => *b,
            DataTypeDescriptor::Int64(b) => *b,
            DataTypeDescriptor::Float32(b) => *b,
            DataTypeDescriptor::Float64(b) => *b,
            DataTypeDescriptor::Boolean(b) => *b,
            DataTypeDescriptor::Binary(b) => *b,
            DataTypeDescriptor::String(b) => *b,
            DataTypeDescriptor::Duration(b, _) => *b,
            DataTypeDescriptor::Time(b, _) => *b,
            DataTypeDescriptor::Date(b, _) => *b,
            DataTypeDescriptor::Datetime(b, _, _, _) => *b,
            DataTypeDescriptor::Null => true,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct CSVData<'a> {
    pub filename: String,
    pub separator: Option<String>,
    pub field_types: HashMap<String, DataTypeDescriptor<'a>>,
    pub delimiter: u8,
    pub max_read_records: Option<usize>,
    pub has_header: bool,
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
            delimiter: (";".as_bytes())[0],
            max_read_records: Some(100),
            has_header: true,
        }
    }
}
