use lalrpop_util::lalrpop_mod;

lalrpop_mod!(pub parser, "/grammar/input.rs"); // synthesized by LALRPOP

#[test]
fn start_parser_test() {
    use crate::grammar::ast::{Ast, CSVData, LoadableFormatData};

    let ast = Ast {
        loadable_filenames: vec![
            LoadableFormatData::CSV(CSVData {
                filename: "f/ga.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "01dg/dfg.parquet".to_string(),
                separator: None,
            }),
        ],
    };

    assert_eq!(
        parser::StartParser::new()
            .parse("load_files CSV(filename=f/ga.csv) CSV(filename=01dg/dfg.parquet)"),
        Ok(ast)
    );
}
