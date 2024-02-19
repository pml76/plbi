use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    character::complete::{char, multispace0},
    combinator::{map, opt},
    error::ParseError,
    multi::many1,
    sequence::{delimited, pair, preceded, separated_pair, tuple},
    IResult, Parser,
};
use polars::datatypes::{DataType, TimeUnit};

use super::ast::{Ast, CSVData, LoadableFormatData};

/// A combinator that takes a parser `inner` and produces a parser that also consumes both leading and
/// trailing whitespace, returning the output of `inner`.
fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl Parser<&'a str, O, E>
where
    F: Parser<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

pub fn ast_parser(input: &str) -> IResult<&str, Ast> {
    map(
        pair(ws(tag("load_files")), ws(file_descriptors_parser)),
        |(_, fs)| Ast {
            loadable_filenames: fs,
        },
    )(input)
}

fn file_descriptors_parser(input: &str) -> IResult<&str, Vec<LoadableFormatData>> {
    many1(file_descriptor_parser)(input)
}

fn file_descriptor_parser(input: &str) -> IResult<&str, LoadableFormatData> {
    let csv_head = tuple((
        ws(tag("CSV")),
        ws(tag("(")),
        ws(tag("file_name")),
        ws(tag("=")),
    ));
    let csv_tail = ws(tag(")"));
    let csv_contents = pair(
        ws(string_parser),
        opt(preceded(ws(tag(",")), ws(schema_parser))),
    );

    let csv_parser = delimited(csv_head, csv_contents, csv_tail);

    let mut csv_map = map(csv_parser, |(f, g)| {
        LoadableFormatData::CSV(CSVData {
            filename: f.to_string(),
            separator: None,
            field_types: g,
        })
    });

    csv_map(input)
}

fn schema_parser(input: &str) -> IResult<&str, HashMap<String, DataType>> {
    let head = tuple((ws(tag("field_types")), ws(tag("{"))));
    let tail = ws(tag("}"));
    let parser = many1(schema_entry_parser);

    let parser_map = map(parser, |entries| {
        let mut ret = HashMap::new();
        for (k, v) in entries {
            ret.insert(k.to_string(), v);
        }
        ret
    });

    delimited(head, parser_map, tail)(input)
}

fn schema_entry_parser(input: &str) -> IResult<&str, (&str, DataType)> {
    let head = ws(tag("("));
    let tail = ws(tag(")"));
    let parser = separated_pair(string_parser, ws(tag(":")), data_type_parser);

    delimited(head, parser, tail)(input)
}

/// strings have the shape (double quotes) string (w/o double quotes) (double quotes)
fn string_parser(input: &str) -> IResult<&str, &str> {
    delimited(char('"'), take_until("\""), char('"'))(input)
}

fn time_unit_parser(input: &str) -> IResult<&str, TimeUnit> {
    let nanoseconds_parser = map(ws(tag("Nanoseconds")), |_| TimeUnit::Nanoseconds);
    let microseconds_parser = map(ws(tag("Microseconds")), |_| TimeUnit::Microseconds);
    let milliseconds_parser = map(ws(tag("Milliseconds")), |_| TimeUnit::Milliseconds);

    alt((nanoseconds_parser, microseconds_parser, milliseconds_parser))(input)
}

/// A parser for data types
fn data_type_parser(input: &str) -> IResult<&str, DataType> {
    let boolean_type_parser = map(ws(tag("Boolean")), |_| DataType::Boolean);
    let string_type_parser = map(ws(tag("String")), |_| DataType::String);
    let uint8_type_parser = map(ws(tag("UInt8")), |_| DataType::UInt8);
    let uint16_type_parser = map(ws(tag("UInt16")), |_| DataType::UInt16);
    let uint32_type_parser = map(ws(tag("UInt32")), |_| DataType::UInt32);
    let uint64_type_parser = map(ws(tag("UInt64")), |_| DataType::UInt64);
    let int8_type_parser = map(ws(tag("Int8")), |_| DataType::Int8);
    let int16_type_parser = map(ws(tag("Int16")), |_| DataType::Int16);
    let int32_type_parser = map(ws(tag("Int32")), |_| DataType::Int32);
    let int64_type_parser = map(ws(tag("Int64")), |_| DataType::Int64);

    let float32_type_parser = map(ws(tag("Float32")), |_| DataType::Float32);
    let float64_type_parser = map(ws(tag("Float64")), |_| DataType::Float64);
    let binary_type_parser = map(ws(tag("Binary")), |_| DataType::Binary);
    let binary_offset_type_parser = map(ws(tag("BinaryOffset")), |_| DataType::BinaryOffset);
    let date_type_parser = map(ws(tag("Date")), |_| DataType::Date);
    let time_type_parser = map(ws(tag("Time")), |_| DataType::Time);
    let null_type_parser = map(ws(tag("Null")), |_| DataType::Null);
    let unknown_type_parser = map(ws(tag("Unknown")), |_| DataType::Unknown);
    //let enum_type_parser = map(ws(tag("Enum")), |_| DataType::Enum);
    // let categorical_type_parser = map(ws(tag("Categorical")),|_| DataType::Categortical)
    let datetime_type_parser = map(pair(ws(tag("Datetime")), ws(time_unit_parser)), |(_, d)| {
        DataType::Datetime(d, None)
    });
    let duration_type_parser = map(pair(ws(tag("Duration")), ws(time_unit_parser)), |(_, d)| {
        DataType::Duration(d)
    });

    alt((
        // enum_type_parser,
        boolean_type_parser,
        uint8_type_parser,
        uint16_type_parser,
        uint32_type_parser,
        uint64_type_parser,
        string_type_parser,
        int8_type_parser,
        int16_type_parser,
        int32_type_parser,
        int64_type_parser,
        float32_type_parser,
        float64_type_parser,
        binary_offset_type_parser,
        binary_type_parser,
        time_type_parser,
        null_type_parser,
        unknown_type_parser,
        datetime_type_parser,
        duration_type_parser,
        date_type_parser,
    ))(input)
}

#[test]
fn ast_parser_test() {
    let mut expected_schema = HashMap::new();
    expected_schema.insert("asdf".to_string(), DataType::Date);

    assert_eq!(
        ast_parser("load_files CSV(file_name = \"dir/fn.csv\", field_types{ (\"asdf\": Date)} )"),
        Ok((
            "",
            Ast {
                loadable_filenames: vec![LoadableFormatData::CSV(CSVData {
                    filename: "dir/fn.csv".to_string(),
                    separator: None,
                    field_types: Some(expected_schema)
                })]
            }
        ))
    );
}

#[test]
fn file_descriptor_parser_test() {
    let mut expected_schema = HashMap::new();
    expected_schema.insert("asdf".to_string(), DataType::Date);

    assert_eq!(
        file_descriptor_parser(
            "CSV ( file_name = \"dir/fn.csv\", field_types{ ( \"asdf\": Date ) } )"
        ),
        Ok((
            "",
            LoadableFormatData::CSV(CSVData {
                filename: "dir/fn.csv".to_string(),
                separator: None,
                field_types: Some(expected_schema)
            })
        ))
    )
}

#[test]
fn schema_entry_parser_test() {
    assert_eq!(
        schema_entry_parser("( \"asdf\": Boolean )"),
        Ok(("", ("asdf", DataType::Boolean)))
    );
}

#[test]
fn schema_parser_test() {
    let mut expected_result = HashMap::new();
    expected_result.insert("asdf".to_string(), DataType::String);

    assert_eq!(
        schema_parser("field_types{ (\"asdf\": String) }"),
        Ok(("", expected_result))
    );
}

#[test]
fn string_parser_test() {
    assert_eq!(
        string_parser("\"Hello, world!\""),
        Ok(("", "Hello, world!"))
    );
}

#[test]
fn data_type_parser_test() {
    assert_eq!(data_type_parser("Boolean"), Ok(("", DataType::Boolean)));
    assert_eq!(data_type_parser("UInt8"), Ok(("", DataType::UInt8)));
    assert_eq!(data_type_parser("UInt16"), Ok(("", DataType::UInt16)));
    assert_eq!(data_type_parser("UInt32"), Ok(("", DataType::UInt32)));
    assert_eq!(data_type_parser("UInt64"), Ok(("", DataType::UInt64)));
    assert_eq!(data_type_parser("Int8"), Ok(("", DataType::Int8)));
    assert_eq!(data_type_parser("Int16"), Ok(("", DataType::Int16)));
    assert_eq!(data_type_parser("Int32"), Ok(("", DataType::Int32)));
    assert_eq!(data_type_parser("Int64"), Ok(("", DataType::Int64)));
    assert_eq!(data_type_parser("Float32"), Ok(("", DataType::Float32)));
    assert_eq!(data_type_parser("Float64"), Ok(("", DataType::Float64)));
    assert_eq!(data_type_parser("String"), Ok(("", DataType::String)));
    assert_eq!(data_type_parser("Binary"), Ok(("", DataType::Binary)));
    assert_eq!(
        data_type_parser("BinaryOffset"),
        Ok(("", DataType::BinaryOffset))
    );
    assert_eq!(data_type_parser("Date"), Ok(("", DataType::Date)));

    assert_eq!(
        data_type_parser("Datetime Nanoseconds"),
        Ok(("", DataType::Datetime(TimeUnit::Nanoseconds, None)))
    );
    assert_eq!(
        data_type_parser("Datetime Microseconds"),
        Ok(("", DataType::Datetime(TimeUnit::Microseconds, None)))
    );
    assert_eq!(
        data_type_parser("Datetime Milliseconds"),
        Ok(("", DataType::Datetime(TimeUnit::Milliseconds, None)))
    );

    assert_eq!(
        data_type_parser("Duration Nanoseconds"),
        Ok(("", DataType::Duration(TimeUnit::Nanoseconds)))
    );
    assert_eq!(
        data_type_parser("Duration Microseconds"),
        Ok(("", DataType::Duration(TimeUnit::Microseconds)))
    );
    assert_eq!(
        data_type_parser("Duration Milliseconds"),
        Ok(("", DataType::Duration(TimeUnit::Milliseconds)))
    );
    assert_eq!(data_type_parser("Time"), Ok(("", DataType::Time)));
    assert_eq!(data_type_parser("Null"), Ok(("", DataType::Null)));
    assert_eq!(data_type_parser("Unknown"), Ok(("", DataType::Unknown)));
}
