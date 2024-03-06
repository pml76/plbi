use std::collections::HashMap;
use arrow::datatypes::TimeUnit;

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



use super::ast::{Ast, CSVData, DataTypeDescriptor, LoadableFormatData};

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
        preceded(ws(tag(",")), ws(schema_parser)),
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

fn schema_parser(input: &str) -> IResult<&str, HashMap<String, DataTypeDescriptor>> {
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

fn schema_entry_parser(input: &str) -> IResult<&str, (&str, DataTypeDescriptor)> {
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
    let nanoseconds_parser = map(ws(tag("Nanoseconds")), |_| TimeUnit::Nanosecond);
    let microseconds_parser = map(ws(tag("Microseconds")), |_| TimeUnit::Microsecond);
    let milliseconds_parser = map(ws(tag("Milliseconds")), |_| TimeUnit::Millisecond);

    alt((nanoseconds_parser, microseconds_parser, milliseconds_parser))(input)
}

/// A parser for data types
fn data_type_parser(input: &str) -> IResult<&str, DataTypeDescriptor> {
    let boolean_type_parser = map(ws(tag("Boolean")), |_| DataTypeDescriptor::Boolean);
    let string_type_parser = map(ws(tag("String")), |_| DataTypeDescriptor::String);
    let uint8_type_parser = map(ws(tag("UInt8")), |_| DataTypeDescriptor::UInt8);
    let uint16_type_parser = map(ws(tag("UInt16")), |_| DataTypeDescriptor::UInt16);
    let uint32_type_parser = map(ws(tag("UInt32")), |_| DataTypeDescriptor::UInt32);
    let uint64_type_parser = map(ws(tag("UInt64")), |_| DataTypeDescriptor::UInt64);
    let int8_type_parser = map(ws(tag("Int8")), |_| DataTypeDescriptor::Int8);
    let int16_type_parser = map(ws(tag("Int16")), |_| DataTypeDescriptor::Int16);
    let int32_type_parser = map(ws(tag("Int32")), |_| DataTypeDescriptor::Int32);
    let int64_type_parser = map(ws(tag("Int64")), |_| DataTypeDescriptor::Int64);

    let float32_type_parser = map(ws(tag("Float32")), |_| DataTypeDescriptor::Float32);
    let float64_type_parser = map(ws(tag("Float64")), |_| DataTypeDescriptor::Float64);
    let binary_type_parser = map(ws(tag("Binary")), |_| DataTypeDescriptor::Binary);
    let binary_offset_type_parser = map(ws(tag("BinaryOffset")), |_| {
        DataTypeDescriptor::BinaryOffset
    });
    let date_type_parser = map(pair(ws(tag("Date")), ws(string_parser)), |(_, f)| {
        DataTypeDescriptor::Date(f)
    });
    let time_type_parser = map(pair(ws(tag("Time")), ws(string_parser)), |(_, f)| {
        DataTypeDescriptor::Time(f)
    });
    let null_type_parser = map(ws(tag("Null")), |_| DataTypeDescriptor::Null);
    let unknown_type_parser = map(ws(tag("Unknown")), |_| DataTypeDescriptor::Unknown);
    let categorical_type_parser = map(ws(tag("Categorical")), |_| DataTypeDescriptor::Categorical);
    let datetime_type_parser = map(
        tuple((
            ws(tag("Datetime")),
            ws(string_parser),
            ws(time_unit_parser),
            ws(opt(string_parser)),
        )),
        |(_, f, d, tz)| DataTypeDescriptor::Datetime(f, d, tz.map(|x| x.to_string())),
    );
    let duration_type_parser = map(pair(ws(tag("Duration")), ws(time_unit_parser)), |(_, d)| {
        DataTypeDescriptor::Duration(d)
    });

    alt((
        categorical_type_parser,
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
    expected_schema.insert("asdf".to_string(), DataTypeDescriptor::Date("%Y"));

    assert_eq!(
        ast_parser(
            "load_files CSV(file_name = \"dir/fn.csv\", field_types{ (\"asdf\": Date \"%Y\")} )"
        ),
        Ok((
            "",
            Ast {
                loadable_filenames: vec![LoadableFormatData::CSV(CSVData {
                    filename: "dir/fn.csv".to_string(),
                    separator: None,
                    field_types: expected_schema,
                })]
            }
        ))
    );
}

#[test]
fn file_descriptor_parser_test() {
    let mut expected_schema = HashMap::new();
    expected_schema.insert("asdf".to_string(), DataTypeDescriptor::Date("%Y"));

    assert_eq!(
        file_descriptor_parser(
            "CSV ( file_name = \"dir/fn.csv\", field_types{ ( \"asdf\": Date \"%Y\") } )"
        ),
        Ok((
            "",
            LoadableFormatData::CSV(CSVData {
                filename: "dir/fn.csv".to_string(),
                separator: None,
                field_types: expected_schema,
            })
        ))
    )
}

#[test]
fn schema_entry_parser_test() {
    assert_eq!(
        schema_entry_parser("( \"asdf\": Boolean )"),
        Ok(("", ("asdf", DataTypeDescriptor::Boolean)))
    );
}

#[test]
fn schema_parser_test() {
    let mut expected_result = HashMap::new();
    expected_result.insert("asdf".to_string(), DataTypeDescriptor::String);

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
    assert_eq!(
        data_type_parser("Boolean"),
        Ok(("", DataTypeDescriptor::Boolean))
    );
    assert_eq!(
        data_type_parser("UInt8"),
        Ok(("", DataTypeDescriptor::UInt8))
    );
    assert_eq!(
        data_type_parser("UInt16"),
        Ok(("", DataTypeDescriptor::UInt16))
    );
    assert_eq!(
        data_type_parser("UInt32"),
        Ok(("", DataTypeDescriptor::UInt32))
    );
    assert_eq!(
        data_type_parser("UInt64"),
        Ok(("", DataTypeDescriptor::UInt64))
    );
    assert_eq!(data_type_parser("Int8"), Ok(("", DataTypeDescriptor::Int8)));
    assert_eq!(
        data_type_parser("Int16"),
        Ok(("", DataTypeDescriptor::Int16))
    );
    assert_eq!(
        data_type_parser("Int32"),
        Ok(("", DataTypeDescriptor::Int32))
    );
    assert_eq!(
        data_type_parser("Int64"),
        Ok(("", DataTypeDescriptor::Int64))
    );
    assert_eq!(
        data_type_parser("Float32"),
        Ok(("", DataTypeDescriptor::Float32))
    );
    assert_eq!(
        data_type_parser("Float64"),
        Ok(("", DataTypeDescriptor::Float64))
    );
    assert_eq!(
        data_type_parser("String"),
        Ok(("", DataTypeDescriptor::String))
    );
    assert_eq!(
        data_type_parser("Binary"),
        Ok(("", DataTypeDescriptor::Binary))
    );
    assert_eq!(
        data_type_parser("BinaryOffset"),
        Ok(("", DataTypeDescriptor::BinaryOffset))
    );
    assert_eq!(
        data_type_parser("Date \"%Y\""),
        Ok(("", DataTypeDescriptor::Date("%Y")))
    );

    assert_eq!(
        data_type_parser("Datetime \"%Y\" Nanoseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime("%Y", TimeUnit::Nanosecond, None)
        ))
    );
    assert_eq!(
        data_type_parser("Datetime \"%Y\" Microseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime("%Y", TimeUnit::Microsecond, None)
        ))
    );
    assert_eq!(
        data_type_parser("Datetime \"%Y\" Milliseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime("%Y", TimeUnit::Millisecond, None)
        ))
    );

    assert_eq!(
        data_type_parser("Duration Nanoseconds"),
        Ok(("", DataTypeDescriptor::Duration(TimeUnit::Nanosecond)))
    );
    assert_eq!(
        data_type_parser("Duration Microseconds"),
        Ok(("", DataTypeDescriptor::Duration(TimeUnit::Microsecond)))
    );
    assert_eq!(
        data_type_parser("Duration Milliseconds"),
        Ok(("", DataTypeDescriptor::Duration(TimeUnit::Millisecond)))
    );
    assert_eq!(
        data_type_parser("Time \"%Y\""),
        Ok(("", DataTypeDescriptor::Time("%Y")))
    );
    assert_eq!(data_type_parser("Null"), Ok(("", DataTypeDescriptor::Null)));
    assert_eq!(
        data_type_parser("Unknown"),
        Ok(("", DataTypeDescriptor::Unknown))
    );
}
