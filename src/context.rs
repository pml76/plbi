use crate::{
    error::PlbiError,
    grammar::ast::{Ast, LoadableFormatData},
};
use polars::frame::DataFrame;
use polars::prelude::*;
use std::{collections::HashMap, ffi::OsStr, path::Path};

pub struct Context {
    pub base_tables: HashMap<String, DataFrame>,
}

impl Context {
    pub fn convert_ast(ast: &Ast) -> Result<Context, PlbiError> {
        let base_tables = load_base_tables(&ast.loadable_filenames)?;
        Ok(Context { base_tables })
    }
}

// load csv, parquet, and json tables...
fn load_base_tables(
    loadable_filenames: &Vec<LoadableFormatData>,
) -> Result<HashMap<String, DataFrame>, PlbiError> {
    let mut ret = HashMap::new();

    for filename in loadable_filenames {
        if let LoadableFormatData::CSV(data) = filename {
            let path = Path::new(&data.filename);
            if !path.exists() {
                let s = format!("{}", path.display());
                return Err(PlbiError::PlbiFileNotfound(s));
            }
            if path.extension() == Some(OsStr::new("csv"))
                || path.extension() == Some(OsStr::new("CSV"))
            {
                let reader = CsvReader::from_path(path);
                if reader.is_err() {
                    let s = format!("{}", path.display());
                    return Err(PlbiError::PlbiCouldNotReadFile(s));
                }

                if data.filename == "contoso/FactOnlineSales.csv" {
                    println!("reading file: {}", path.display());
                }

                let df = reader.unwrap().has_header(true).finish();
                if df.is_err() {
                    let polars_error = df.err().unwrap();
                    println!("{}", polars_error);

                    let s = format!("{}", path.display());
                    return Err(PlbiError::PlbiCouldNotReadFile(s));
                }
                if path.file_stem().is_none() {
                    let s = format!("{}", path.display());
                    return Err(PlbiError::PlbiFileNameWithoutStem(s));
                }
                ret.insert(
                    path.file_stem().unwrap().to_str().unwrap().to_string(),
                    df.unwrap(),
                );
                continue;
            }
        }

        /*         if path.extension() == Some(OsStr::new("parquet")) {
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
        */
    }

    Ok(ret)
}

#[test]
fn generate_context_test() {
    use crate::grammar::ast::*;

    let ast = Ast {
        loadable_filenames: vec![
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimAccount.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimChannel.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimCurrency.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimCustomer.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimDate.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimEmployee.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimEntity.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimGeography.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimMachine.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimOutage.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProduct.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProductCategory.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProductSubcategory.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimPromotion.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimSalesTerritory.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimScenario.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimStore.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactExchangeRate.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactInventory.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactITMachine.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactITSLA.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactOnlineSales.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactSales.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactSalesQuota.csv".to_string(),
                separator: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactStrategyPlan.csv".to_string(),
                separator: None,
            }),
        ],
    };

    assert!(Context::convert_ast(&ast).is_ok());
}
