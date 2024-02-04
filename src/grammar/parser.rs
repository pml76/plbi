

use lalrpop_util::lalrpop_mod;

lalrpop_mod!(pub parser, "/grammar/input.rs"); // synthesized by LALRPOP

#[test]
fn start_parser_test() {
    use crate::grammar::ast::Ast;

    let ast = Ast{ loadable_filenames: vec!("f/ga.csv".to_string(), "01dg/dfg.parquet".to_string())
    };
    assert_eq!(parser::StartParser::new().parse("load_files f/ga.csv 01dg/dfg.parquet"), Ok(ast));
}
