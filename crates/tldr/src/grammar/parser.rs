/// This module is a parser for the data modelling language of tldr.
/// 
/// Every tldr script has to read in somedata first
/// 
/// tldr_input_language     = "load_files" file_descriptor ["," file_descriptor]* ","?
/// 
/// file_descriptor         = "(" name_parameter_block ["," name_parameter_block]* ","? ")"
/// 
/// name_parameter_block    = "csv_file_name" ":" \"file_path\"
///                         | "delimiter" ":" \"char\"
///                         | "has_header" ":" (true|false)
///                         | "max_read_records" ":" number
///                         | "field_types" ":" "(" field_type_descriptor ["," field_type_descriptor]* ","? ")"
///
///     >>> a csv_file_name block must be given
///     >>> a delimiter block gives the separator of values in the file
///     >>> a has_header block indicates whether the file contains headers or not
///     >>> a max_read_records block gives the number of rows to be read to determine the schema
///     >>> a field_types block can overwrite some or all entries on the schema
/// 
/// field_type_descriptor   = "name" ":" \"name\"
///                         | "type" ":" type 
/// 
/// type                    = "int8"  ["(" "is_nullable" ":" (true|false) ")"]?
///                         | "int16" (true|false)
///                         | "int32" (true|false)
///                         | "int64" (true|false)
///                         | "uint8" (true|false)
///                         | "uint16" (true|flase)
///                         | "uint32" (true|false)
///                         | "uint64" (true|false)
///                         | "float32" (true|false)
///                         | "float64" (true|false)
///                         | "null"
///                         | "boolean" (true|false)
///                         | "binary" (true|false)
///                         | "string" (true|false)
///                         | "duration" (true|false) time_unit
///                         | "time" (true|false) \"format_string\"
///                         | "date" (true|false) \"format_string\"
///                         | "datetime" (true|false) \"format_string\"
/// 
///     >>> the first parameter indicates whether the field is nullable or not
/// 

use arrow::{compute::is_null, datatypes::TimeUnit};
use std::collections::HashMap;

use nom::{
    branch::{alt, permutation}, 
    bytes::complete::{tag, take_until}, 
    character::{ complete::{char, multispace0, u32}, 
                 streaming::anychar}, 
                 combinator::{map, opt}, 
                 error::ParseError, multi::{many1, separated_list1}, 
                 sequence::{delimited, separated_pair, tuple}, 
    IResult, Parser
};

use super::ast::{Ast, CSVData, DataTypeDescriptor, FileDescriptorData};

/// A combinator that takes a parser `inner` and produces a parser that also consumes both leading and
/// trailing whitespace, returning the output of `inner`.
fn ws<'a, F, O, E: ParseError<&'a str>>(inner: F) -> impl Parser<&'a str, O, E>
where
    F: Parser<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}


/// entry point of the parser. 
/// 
/// Here we encode the rule for tldr_input_language:
/// 
/// tldr_input_language     = "load_files" file_descriptor ["," file_descriptor]* ","?
/// 
pub fn ast_parser(input: &str) -> IResult<&str, Ast> {
    let start = ws(tag("load_files"));
    let mid = ws(separated_list1(ws(tag(",")), ws(file_descriptor_parser)));
    let end = ws(opt(tag(",")));

    let parser = delimited(start, mid, end);

    map( parser, |file_descriptors| Ast{ file_descriptors})(input)

}



/// Here, we parse the entries of the file-descriptor block
/// 
/// name_parameter_block    = "csv_file_name" ":" \"file_path\"
///                         | "delimiter" ":" \"char\"
///                         | "has_header" ":" (true|false)
///                         | "max_read_records" ":" number
///                         | "field_types" ":" "(" field_type_descriptor ["," field_type_descriptor]* ","? ")"
/// 
fn file_descriptor_parser(input: &str) -> IResult<&str, FileDescriptorData> {
    
    let csv_file_name_block = map(
        tuple((ws(tag("csv_file_name")), ws(tag(":")), ws(string_parser))),
        |(_,_,s)| s
    );

    let delimiter_block = map(
        tuple((ws(tag("delimiter")), ws(tag(":")), ws(char_parser))),
        |(_,_,c)| c
    );
    
    let has_header_block = map(
        tuple((ws(tag("has_header")), ws(tag(":")), ws(bool_parser))),
        |(_,_,b)| b
    );

    let max_read_records_block = map (
        tuple((ws(tag("max_read_records")), ws(tag(":")), ws(opt(usize_parser)))),
        |(_,_,n)| n
    );

    let field_types_block = map(
        tuple((ws(tag("field_types")), ws(tag(":")), ws(schema_parser))),
        |(_,_,s)| s
    );

    let any_of_that = permutation((
        csv_file_name_block, 
        ws(opt(field_types_block)),
        ws(opt(delimiter_block)), 
        ws(opt(has_header_block)), 
        ws(opt(max_read_records_block)), 
    ));

    let mut csv_file_path: &str = "";
    let mut field_types: HashMap<&str, DataTypeDescriptor<'_>> = HashMap::new();
    let mut delimiter: u8 = b',';
    let mut has_headers = true;
    let mut max_read_records = Some(100);

    map(any_of_that, 
        |( csv_file_path, 
              field_types, 
              delimiter, 
              has_header, 
              max_read_records)| {
            FileDescriptorData::CSV(CSVData{
                csv_file_path,
                field_types: { 
                    if let Some(f) = field_types { 
                        f 
                    } else {
                        HashMap::new()
                    } 
                },
                delimiter: {
                    if let Some(d) = delimiter {
                        d
                    } else {
                        b','
                    }
                },
                max_read_records: {
                    if let Some(r)= max_read_records {
                        r
                    } else {
                        Some(100)
                    }
                },
                has_header: {
                    if let Some(b) = has_header {
                        b
                    } else {
                        true
                    }
                },
            }
        )})(input)


}

fn schema_parser<'a>(input: &str) -> IResult<&str, HashMap<&'a str, DataTypeDescriptor>> {
    let head = tuple((ws(tag("field_types")), ws(tag("("))));
    let tail = tuple((opt(ws(tag(","))), ws(tag(")"))));
    let parser = 
        separated_list1(
            ws(tag(",")),
            schema_entry_parser
        );

    let parser_map = map(parser, |entries| {
        let mut ret = HashMap::new();
        for (k, v) in entries {
            ret.insert(k, v);
        }
        ret
    });

    delimited(head, parser_map, tail)(input)
}

fn schema_entry_parser(input: &str) -> IResult<&str, (&str, DataTypeDescriptor)> {
    separated_pair(
        string_parser, 
        ws(tag(":")), 
        data_type_parser
    )(input)
}

/// strings have the shape (double quotes) string (w/o double quotes) (double quotes)
fn string_parser(input: &str) -> IResult<&str, &str> {
    delimited(char('"'), take_until("\""), char('"'))(input)
}

/// parse a single char in double quotes 
fn char_parser(input: &str) -> IResult<&str, u8> {
    delimited(char('"'), anychar, char('"'))
        (input)
        .map(|(s,c)| (s,c as u8))
}

fn usize_parser(input: &str) -> IResult<&str, usize> {
    u32(input).map(|(s,d)| (s,d as usize))
}

fn time_unit_parser(input: &str) -> IResult<&str, TimeUnit> {
    let nanoseconds_parser = map(ws(tag("Nanoseconds")), |_| TimeUnit::Nanosecond);
    let microseconds_parser = map(ws(tag("Microseconds")), |_| TimeUnit::Microsecond);
    let milliseconds_parser = map(ws(tag("Milliseconds")), |_| TimeUnit::Millisecond);

    alt((nanoseconds_parser, microseconds_parser, milliseconds_parser))(input)
}

fn is_nullable_parser(input: &str) -> IResult<&str, bool> {
    map(
        tuple((ws(tag("is_nullable")), ws(tag(":")), ws(bool_parser))),
        |(_, _, b)| b,
    )(input)
}

fn format_parser(input: &str) -> IResult<&str, &str> {
    map(
        tuple((ws(tag("format")), ws(tag(":")), ws(string_parser))),
        |(_,_,s)| s
    )(input)
}

/// A parser for data types
fn data_type_parser(input: &str) -> IResult<&str, DataTypeDescriptor> {

    let is_nullable_parameter_parser = map(opt(delimited(
            ws(tag("(")),
            ws(is_nullable_parser),
            ws(tag(")")),
        )),
    |b|{ 
            if let Some(b) = b  {
                b
            } else {
                true
            }
        },
    );

    let boolean_type_parser = map(
        tuple((ws(tag("boolean")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Boolean(b),
    );
    let string_type_parser = map(
        tuple((ws(tag("string")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::String(b),
    );
    let uint8_type_parser = map(
        tuple((ws(tag("uint8")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::UInt8(b),
    );
    let uint16_type_parser = map(
        tuple((ws(tag("uint16")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::UInt16(b),
    );
    let uint32_type_parser = map(
        tuple((ws(tag("uint32")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::UInt32(b),
    );
    let uint64_type_parser = map(
        tuple((ws(tag("uint64")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::UInt64(b),
    );
    let int8_type_parser = map(
        tuple((ws(tag("int8")), ws(is_nullable_parser))),
        |(_, b)| DataTypeDescriptor::Int8(b),
    );
    let int16_type_parser = map(
        tuple((ws(tag("int16")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Int16(b),
    );
    let int32_type_parser = map(
        tuple((ws(tag("int32")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Int32(b),
    );
    let int64_type_parser = map(
        tuple((ws(tag("int64")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Int64(b),
    );

    let float32_type_parser = map(
        tuple((ws(tag("float32")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Float32(b),
    );
    let float64_type_parser = map(
        tuple((ws(tag("float64")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Float64(b),
    );
    let binary_type_parser = map(
        tuple((ws(tag("binary")), ws(is_nullable_parameter_parser))),
        |(_, b)| DataTypeDescriptor::Binary(b),
    );
    let date_type_parser = map(
        tuple((ws(tag("date")), ws(string_parser), ws(is_nullable_parser))),
        |(_, f, b)| DataTypeDescriptor::Date(b, f),
    );
    let time_type_parser = map(
        tuple((ws(tag("time")), ws(string_parser), ws(is_nullable_parser))),
        |(_, f, b)| DataTypeDescriptor::Time(b, f),
    );
    let null_type_parser = map(ws(tag("null")), |_| DataTypeDescriptor::Null);
    let datetime_type_parser = map(
        tuple((
            ws(tag("datetime")),
            ws(string_parser),
            ws(time_unit_parser),
            ws(opt(string_parser)),
            ws(is_nullable_parser),
        )),
        |(_, f, d, tz, b)| DataTypeDescriptor::Datetime(b, f, d, tz.map(|x| x.to_string())),
    );
    let duration_type_parser = map(
        tuple((
            ws(tag("duration")),
            ws(time_unit_parser),
            ws(is_nullable_parser),
        )),
        |(_, d, b)| DataTypeDescriptor::Duration(b, d),
    );

    alt((
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
        binary_type_parser,
        time_type_parser,
        null_type_parser,
        datetime_type_parser,
        duration_type_parser,
        date_type_parser,
    ))(input)
}

fn bool_parser(i: &str) -> IResult<&str, bool> {
    alt((map(tag("true"), |_| true), map(tag("false"), |_| false))).parse(i)
}

#[test]
fn ast_parser_test() {
    let mut expected_schema = HashMap::new();
    expected_schema.insert("asdf", DataTypeDescriptor::Date(false, "%Y"));

    assert_eq!(
        ast_parser(
            "load_files (csv_file_name = \"dir/fn.csv\", field_types{ (\"asdf\": Date \"%Y\" is_nullable: false)} )"
        ),
        Ok((
            "",
            Ast {
                file_descriptors: vec![FileDescriptorData::CSV(
                    CSVData {
                        csv_file_path:"dir/fn.csv",
                        field_types: expected_schema, 
                        delimiter: b';', 
                        max_read_records: Some(100), 
                        has_header: true 
                    })]
            }
        ))
    );
}

#[test]
fn file_descriptor_parser_test() {
    let mut expected_schema = HashMap::new();
    expected_schema.insert("asdf", DataTypeDescriptor::Date(false, "%Y"));

    assert_eq!(
        file_descriptor_parser(
            "CSV ( file_name = \"dir/fn.csv\", field_types{ ( \"asdf\": Date \"%Y\") } )"
        ),
        Ok((
            "",
            FileDescriptorData::CSV(CSVData {
                csv_file_path:"dir/fn.csv",
                field_types: expected_schema, 
                delimiter: b';', 
                max_read_records: Some(100), 
                has_header: true 
            })
        ))
    )
}

#[test]
fn schema_entry_parser_test() {
    assert_eq!(
        schema_entry_parser("( \"asdf\": Boolean )"),
        Ok(("", ("asdf", DataTypeDescriptor::Boolean(false))))
    );
}

#[test]
fn schema_parser_test() {
    let mut expected_result = HashMap::new();
    expected_result.insert("asdf", DataTypeDescriptor::String(false));

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
        Ok(("", DataTypeDescriptor::Boolean(false)))
    );
    assert_eq!(
        data_type_parser("UInt8"),
        Ok(("", DataTypeDescriptor::UInt8(false)))
    );
    assert_eq!(
        data_type_parser("UInt16"),
        Ok(("", DataTypeDescriptor::UInt16(false)))
    );
    assert_eq!(
        data_type_parser("UInt32"),
        Ok(("", DataTypeDescriptor::UInt32(false)))
    );
    assert_eq!(
        data_type_parser("UInt64"),
        Ok(("", DataTypeDescriptor::UInt64(false)))
    );
    assert_eq!(
        data_type_parser("Int8"),
        Ok(("", DataTypeDescriptor::Int8(false)))
    );
    assert_eq!(
        data_type_parser("Int16"),
        Ok(("", DataTypeDescriptor::Int16(false)))
    );
    assert_eq!(
        data_type_parser("Int32"),
        Ok(("", DataTypeDescriptor::Int32(false)))
    );
    assert_eq!(
        data_type_parser("Int64"),
        Ok(("", DataTypeDescriptor::Int64(false)))
    );
    assert_eq!(
        data_type_parser("Float32"),
        Ok(("", DataTypeDescriptor::Float32(false)))
    );
    assert_eq!(
        data_type_parser("Float64"),
        Ok(("", DataTypeDescriptor::Float64(false)))
    );
    assert_eq!(
        data_type_parser("String"),
        Ok(("", DataTypeDescriptor::String(false)))
    );
    assert_eq!(
        data_type_parser("Binary"),
        Ok(("", DataTypeDescriptor::Binary(false)))
    );
    assert_eq!(
        data_type_parser("Date \"%Y\""),
        Ok(("", DataTypeDescriptor::Date(false, "%Y")))
    );

    assert_eq!(
        data_type_parser("Datetime \"%Y\" Nanoseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime(false, "%Y", TimeUnit::Nanosecond, None)
        ))
    );
    assert_eq!(
        data_type_parser("Datetime \"%Y\" Microseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime(false, "%Y", TimeUnit::Microsecond, None)
        ))
    );
    assert_eq!(
        data_type_parser("Datetime \"%Y\" Milliseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime(false, "%Y", TimeUnit::Millisecond, None)
        ))
    );

    assert_eq!(
        data_type_parser("Duration Nanoseconds"),
        Ok((
            "",
            DataTypeDescriptor::Duration(false, TimeUnit::Nanosecond)
        ))
    );
    assert_eq!(
        data_type_parser("Duration Microseconds"),
        Ok((
            "",
            DataTypeDescriptor::Duration(false, TimeUnit::Microsecond)
        ))
    );
    assert_eq!(
        data_type_parser("Duration Milliseconds"),
        Ok((
            "",
            DataTypeDescriptor::Duration(false, TimeUnit::Millisecond)
        ))
    );
    assert_eq!(
        data_type_parser("Time \"%Y\""),
        Ok(("", DataTypeDescriptor::Time(false, "%Y")))
    );
    assert_eq!(data_type_parser("Null"), Ok(("", DataTypeDescriptor::Null)));
}
