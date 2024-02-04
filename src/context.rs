use crate::{error::PlbiError, grammar::ast::Ast};
use polars::frame::DataFrame;
use polars::prelude::*;
use std::{ffi::OsStr, path::Path, string::String};

pub struct Context {
    pub base_tables: Vec<DataFrame>,
}

impl Context {
    pub fn convert_ast(ast: Ast) -> Result<Context, PlbiError> {
        let base_tables = load_base_tables(&ast.loadable_filenames)?;
        Ok(Context { base_tables })
    }
}

// load csv, parquet, and json tables...
fn load_base_tables(loadable_filenames: &Vec<String>) -> Result<Vec<DataFrame>, PlbiError> {
    let mut ret = Vec::new();

    for filename in loadable_filenames {
        let path = Path::new(filename);
        if !path.exists() {
            return Err(PlbiError::PlbiFileNotfound);
        }
        if path.extension() == Some(OsStr::new("csv"))
            || path.extension() == Some(OsStr::new("CSV"))
        {
            let reader = CsvReader::from_path(path);
            if reader.is_err() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiCouldNotReadFile(s));
            }
            let df = reader.unwrap().has_header(true).finish();
            if df.is_err() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiCouldNotReadFile(s));
            }
            ret.push(df.unwrap());

            continue;
        }

        if path.extension() == Some(OsStr::new("parquet")) {
            let file = std::fs::File::open(path);
            if file.is_err() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiCouldNotReadFile(s));
            }
            let df = ParquetReader::new(&mut file.unwrap()).finish();
            if df.is_err() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiCouldNotReadFile(s));
            }
            ret.push(df.unwrap());

            continue;
        }

        if path.extension() == Some(OsStr::new("json")) {
            let file = std::fs::File::open(path);
            if file.is_err() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiCouldNotReadFile(s));
            }

            let df = JsonReader::new(&mut file.unwrap()).finish();
            if df.is_err() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiCouldNotReadFile(s));
            }
            ret.push(df.unwrap());

            continue;
        }
    }

    Ok(ret)
}

