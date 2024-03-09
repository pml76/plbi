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

use arrow::datatypes::TimeUnit;
use std::collections::HashMap;

use nom::{
    branch::alt, 
    bytes::complete::{tag, take_until}, 
    character::{ complete::{char, multispace0, u32}, 
                 streaming::anychar}, 
                 combinator::{map, opt}, 
                 error::ParseError, multi::{many0, separated_list1}, 
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

    enum IntermediateResult<'a> {
        CSVFileName(&'a str),
        Delimiter(u8),
        HasHeader(bool),
        MaxReadRecords(Option<usize>),
        FieldTypes(HashMap<&'a str, DataTypeDescriptor<'a>>),
    }
    
    let csv_file_name_block = map(
        tuple((
            ws(tag("csv_file_name")), 
            ws(tag(":")), 
            ws(string_parser),
            opt(ws(tag(","))),
        )),
        |(_,_,s,_)| IntermediateResult::CSVFileName(s)
    );

    let delimiter_block = map(
        tuple((
            ws(tag("delimiter")), 
            ws(tag(":")), 
            ws(char_parser),
            opt(ws(tag(","))),

        )),
        |(_,_,c,_)| IntermediateResult::Delimiter(c)
    );
    
    let has_header_block = map(
        tuple((
            ws(tag("has_header")), 
            ws(tag(":")), 
            ws(bool_parser),
            opt(ws(tag(","))),
        )),
        |(_,_,b,_)| IntermediateResult::HasHeader(b)
    );

    let max_read_records_block = map (
        tuple((
            ws(tag("max_read_records")), 
            ws(tag(":")), 
            ws(opt(usize_parser)),
            opt(ws(tag(","))),
        )),
        |(_,_,n,_)| IntermediateResult::MaxReadRecords(n)
    );

    let field_types_block = map(
        tuple((
            ws(tag("field_types")), 
            ws(tag(":")), 
            ws(schema_parser),
            opt(ws(tag(","))),
        )),
        |(_,_,s,_)| IntermediateResult::FieldTypes(s)
    );

    let any_of_that = many0(alt((
        csv_file_name_block, 
        field_types_block,
        delimiter_block, 
        has_header_block, 
        max_read_records_block, 
    )));



    map(opt(any_of_that), 
        |ds| {
            let mut csv_file_path: &str = "";
            let mut field_types: HashMap<&str, DataTypeDescriptor<'_>> = HashMap::new();
            let mut delimiter: u8 = b',';
            let mut has_header = true;
            let mut max_read_records = Some(100);

            if let Some(ds) = ds { 
                for d in ds {
                    match d {
                        IntermediateResult::CSVFileName(s) => csv_file_path = s,
                        IntermediateResult::Delimiter(c) => delimiter = c,
                        IntermediateResult::FieldTypes(s) => field_types = s,
                        IntermediateResult::HasHeader(b) => has_header = b,
                        IntermediateResult::MaxReadRecords(n) => max_read_records = n,
                    }
                }
            }

            FileDescriptorData::CSV(CSVData{
                csv_file_path,
                field_types,
                delimiter,
                max_read_records,
                has_header,
            })

        }
          
        )(input)


}

fn schema_parser(input: &str) -> IResult<&str, HashMap<&str, DataTypeDescriptor>> {
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

    map(
        tuple((
            ws(tag("time_unit")),
            ws(tag(":")),
            ws(alt((nanoseconds_parser, microseconds_parser, milliseconds_parser))),
        )),
        |(_,_,b)| b
    )(input)
}

fn is_nullable_parser(input: &str) -> IResult<&str, bool> {
    map(
        tuple((
            ws(tag("is_nullable")), 
            ws(tag(":")), 
            ws(bool_parser), 
            opt(ws(tag(",")))
        )),
        |(_, _, b,_)| b,
    )(input)
}


fn is_nullable_parameter_parser(input: &str) -> IResult<&str, bool> {
    map(opt(delimited(
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
    )(input)
}

fn format_parser(input: &str) -> IResult<&str, &str> {
    map(
        tuple((
            ws(tag("format")), 
            ws(tag(":")), 
            ws(string_parser),
            opt(ws(tag(","))),
        )),
        |(_,_,s,_)| s
    )(input)
}

fn time_unit_parameter_parser(input: &str) -> IResult<&str, (TimeUnit, bool)> {
    enum IntermediateResult {
        Bool(bool),
        TimeUnit(TimeUnit),        
    }

    let time_unit_parser_intermediate = map (
        time_unit_parser, IntermediateResult::TimeUnit
    );

    let is_nullable_parser_intermediate = map (
        is_nullable_parser, IntermediateResult::Bool
    );

    let p = many0(
        alt((
            is_nullable_parser_intermediate,
            time_unit_parser_intermediate
        ))
    );

    map(opt(delimited(
        ws(tag("(")),
        ws(p),
        ws(tag(")")),
    )), |ds| {
        let mut b = true;
        let mut s = TimeUnit::Microsecond;
        if let Some(ds) = ds {
            for d in ds {
                if let IntermediateResult::Bool(b2) = d {
                    b = b2;
                }
                if let IntermediateResult::TimeUnit(s2) = d {
                    s = s2;
                }
            }
        }
        (s, b)
    } )(input)

}


fn format_parameter_parser(input: &str) -> IResult<&str, (&str, bool)> {
    enum IntermediateResult<'a> {
        Bool(bool),
        Str(&'a str),        
    }

    let format_parser_intermediate = map(
        format_parser, IntermediateResult::Str
    );

    let is_nullable_intermediate = map(
        is_nullable_parser,  IntermediateResult::Bool
    );

    let p = many0(
        alt((
            format_parser_intermediate, 
            is_nullable_intermediate,
        ))
    );


    map(opt(delimited(
        ws(tag("(")),
        ws(p),
        ws(tag(")")),
    )), |ds| {
        let mut b = true;
        let mut s = "";
        if let Some(ds) = ds {
            for d in ds {
                if let IntermediateResult::Bool(b2) = d {
                    b = b2;
                }
                if let IntermediateResult::Str(s2) = d {
                    s = s2;
                }
            }
        }
        (s, b)
    } )(input)
}

#[test]
fn format_parameter_parser_test() {
    assert_eq!(format_parameter_parser(""), Ok(("",("",true))));
    assert_eq!(format_parameter_parser("()"), Ok(("",("",true))));
    assert_eq!(format_parameter_parser("(format : \"%Y\", is_nullable: false)"), Ok(("",("%Y",false))));
    assert_eq!(format_parameter_parser("(is_nullable: false, format : \"%Y\")"), Ok(("",("%Y",false))));
}

/// A parser for data types
fn data_type_parser(input: &str) -> IResult<&str, DataTypeDescriptor> {

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
        tuple((ws(tag("date")), ws(format_parameter_parser))),
        |(_, (f, b))| DataTypeDescriptor::Date(b, f),
    );
    
    let time_type_parser = map(
        tuple((ws(tag("time")), ws(format_parameter_parser))),
        |(_, (f, b))| DataTypeDescriptor::Time(b, f),
    );
    
    let null_type_parser = map(
        ws(tag("null")), 
        |_| DataTypeDescriptor::Null
    );
    
    let datetime_type_parser = map(
        tuple((
            ws(tag("datetime")),
            ws(format_parameter_parser),
        )),
        |(_, (f, b))| DataTypeDescriptor::Datetime(b, f),
    );
    let duration_type_parser = map(
        tuple((
            ws(tag("duration")),
            ws(time_unit_parameter_parser),
        )),
        |(_, (d, b))| DataTypeDescriptor::Duration(b, d),
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
            DataTypeDescriptor::Datetime(false, "%Y")
        ))
    );
    assert_eq!(
        data_type_parser("Datetime \"%Y\" Microseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime(false, "%Y")
        ))
    );
    assert_eq!(
        data_type_parser("Datetime \"%Y\" Milliseconds"),
        Ok((
            "",
            DataTypeDescriptor::Datetime(false, "%Y")
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
