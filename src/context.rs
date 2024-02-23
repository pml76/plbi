use crate::{
    error::TldrError,
    grammar::ast::{Ast, DataTypeDescriptor, LoadableFormatData},
};
use polars::{
    frame::DataFrame,
    lazy::{
        dsl::{col, StrptimeOptions},
        frame::IntoLazy,
    },
    prelude::{AnyValue, Arc, CsvReader, DataType, Field, Schema, SerReader},
};

use std::{collections::HashMap, ffi::OsStr, path::Path};

pub struct TableColumn<'a> {
    pub table: &'a str,
    pub column: &'a str,
}

pub struct Context<'a> {
    pub base_tables: HashMap<String, DataFrame>,
    pub filter_context: HashMap<TableColumn<'a>, AnyValue<'a>>,
}

impl<'a> Context<'a> {
    pub fn convert_ast(ast: &'a Ast) -> Result<Context<'a>, TldrError> {
        let base_tables = load_base_tables(&ast.loadable_filenames)?;
        let filter_context: HashMap<TableColumn<'a>, AnyValue<'a>> = HashMap::new();
        Ok(Context {
            base_tables,
            filter_context,
        })
    }
}

// load csv, parquet, and json tables...
fn load_base_tables(
    loadable_filenames: &Vec<LoadableFormatData>,
) -> Result<HashMap<String, DataFrame>, TldrError> {
    let mut ret = HashMap::new();

    for filename in loadable_filenames {
        if let LoadableFormatData::CSV(data) = filename {
            let path = Path::new(&data.filename);
            if !path.exists() {
                let s = format!("{}", path.display());
                return Err(TldrError::TldrFileNotfound(s));
            }
            if path.extension() == Some(OsStr::new("csv"))
                || path.extension() == Some(OsStr::new("CSV"))
            {
                let reader = CsvReader::from_path(path);
                if reader.is_err() {
                    let s = format!("{}", path.display());
                    return Err(TldrError::TldrCouldNotReadFile(s));
                }

                println!("reading file: {}", path.display());

                // get the types right ...
                let schema = data.field_types.as_ref().map(|element| {
                    element
                        .iter()
                        .map(|(k, v)| {
                            let dtype = match v {
                                &DataTypeDescriptor::Time(_)
                                | &DataTypeDescriptor::Date(_)
                                | &DataTypeDescriptor::Datetime(_, _, _) => DataType::String,
                                &DataTypeDescriptor::Categorical => {
                                    DataType::Categorical(None, Default::default())
                                }
                                &DataTypeDescriptor::UInt8 => DataType::UInt8,
                                &DataTypeDescriptor::UInt16 => DataType::UInt16,
                                &DataTypeDescriptor::UInt32 => DataType::UInt32,
                                &DataTypeDescriptor::UInt64 => DataType::UInt64,
                                &DataTypeDescriptor::Int8 => DataType::Int8,
                                &DataTypeDescriptor::Int16 => DataType::Int16,
                                &DataTypeDescriptor::Int32 => DataType::Int32,
                                &DataTypeDescriptor::Int64 => DataType::Int64,
                                &DataTypeDescriptor::Float32 => DataType::Float32,
                                &DataTypeDescriptor::Float64 => DataType::Float64,
                                &DataTypeDescriptor::String => DataType::String,
                                &DataTypeDescriptor::Binary => DataType::Binary,
                                &DataTypeDescriptor::BinaryOffset => DataType::BinaryOffset,
                                &DataTypeDescriptor::Duration(tu) => DataType::Duration(tu),
                                &DataTypeDescriptor::Boolean => DataType::Boolean,
                                &DataTypeDescriptor::Unknown => DataType::Unknown,
                                &DataTypeDescriptor::Null => DataType::Null,
                            };
                            Field::new(k, dtype)
                        })
                        .collect::<Schema>()
                });

                let df = reader
                    .unwrap()
                    .has_header(true) // Assume the file has headers
                    .with_try_parse_dates(true) // try to read dates as such
                    .with_dtypes(schema.map(Arc::new))
                    // .infer_schema(None) // Scan entire file to determine the schema
                    .finish();

                if df.is_err() {
                    let polars_error = df.err().unwrap();
                    println!("{}", polars_error);

                    let s = format!("{}", path.display());
                    return Err(TldrError::TldrCouldNotReadFile(s));
                }

                let mut df = df.unwrap();
                let schema = df.schema();

                if data.field_types.is_some() {
                    for (field_name, field_type) in data.field_types.as_ref().unwrap() {
                        if schema.get_field(field_name).is_none() {
                            continue;
                        }
                        if let DataTypeDescriptor::Date(f) = field_type {
                            let q1 = col(field_name).str().to_date(StrptimeOptions {
                                format: Some(f.to_string()),
                                ..Default::default()
                            });
                            let q2 = col("*").exclude([field_name]);

                            let tmp = df.clone().lazy().select([q1, q2]).collect();
                            if tmp.is_err() {
                                continue;
                            }
                            df = tmp.unwrap();
                        }
                    }
                }

                if path.file_stem().is_none() {
                    let s = format!("{}", path.display());
                    return Err(TldrError::TldrFileNameWithoutStem(s));
                }
                ret.insert(path.file_stem().unwrap().to_str().unwrap().to_string(), df);
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

    let mut online_sales_field_types = HashMap::new();
    online_sales_field_types.insert("SalesOrderNumber".to_string(), DataTypeDescriptor::String);

    let ast = Ast {
        loadable_filenames: vec![
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimAccount.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimChannel.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimCurrency.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimCustomer.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimDate.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimEmployee.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimEntity.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimGeography.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimMachine.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimOutage.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProduct.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProductCategory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProductSubcategory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimPromotion.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimSalesTerritory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimScenario.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimStore.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactExchangeRate.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactInventory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactITMachine.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactITSLA.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactOnlineSales.csv".to_string(),
                separator: None,
                field_types: Some(online_sales_field_types),
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactSales.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactSalesQuota.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactStrategyPlan.csv".to_string(),
                separator: None,
                field_types: None,
            }),
        ],
    };

    assert!(Context::convert_ast(&ast).is_ok());
}

#[test]
fn date_format_test() {
    use crate::grammar::{ast::*, parser::ast_parser};
    let string_to_parse = "load_files 
    CSV(file_name = \"contoso/DimDate.csv\", field_types{ (\"DateKey\": Date \"%Y-%m-%d\") })
    ";

    let parse_result = ast_parser(string_to_parse);

    assert!(parse_result.is_ok());

    let mut dim_date_field_types = HashMap::new();
    dim_date_field_types.insert("DateKey".to_string(), DataTypeDescriptor::Date("%Y-%m-%d"));

    let expected_ast = Ast {
        loadable_filenames: vec![LoadableFormatData::CSV(CSVData {
            filename: "contoso/DimDate.csv".to_string(),
            separator: None,
            field_types: Some(dim_date_field_types),
        })],
    };

    assert_eq!(parse_result, Ok(("", expected_ast)));

    let (_, ast) = parse_result.unwrap();
    assert!(Context::convert_ast(&ast).is_ok());
}

#[test]
fn parse_to_context_test() {
    use crate::grammar::{ast::*, parser::ast_parser};

    let string_to_parse = "load_files 
    CSV(file_name = \"contoso/DimAccount.csv\")
    CSV(file_name = \"contoso/DimChannel.csv\")
    CSV(file_name = \"contoso/DimCurrency.csv\")
    CSV(file_name = \"contoso/DimCustomer.csv\")
    CSV(file_name = \"contoso/DimDate.csv\")
    CSV(file_name = \"contoso/DimEmployee.csv\")
    CSV(file_name = \"contoso/DimEntity.csv\")
    CSV(file_name = \"contoso/DimGeography.csv\")
    CSV(file_name = \"contoso/DimMachine.csv\")
    CSV(file_name = \"contoso/DimOutage.csv\")
    CSV(file_name = \"contoso/DimProduct.csv\")
    CSV(file_name = \"contoso/DimProductCategory.csv\")
    CSV(file_name = \"contoso/DimProductSubcategory.csv\")
    CSV(file_name = \"contoso/DimPromotion.csv\")
    CSV(file_name = \"contoso/DimSalesTerritory.csv\")
    CSV(file_name = \"contoso/DimScenario.csv\")
    CSV(file_name = \"contoso/DimStore.csv\")
    CSV(file_name = \"contoso/FactExchangeRate.csv\")
    CSV(file_name = \"contoso/FactInventory.csv\")
    CSV(file_name = \"contoso/FactITMachine.csv\")
    CSV(file_name = \"contoso/FactITSLA.csv\")
    CSV(file_name = \"contoso/FactOnlineSales.csv\", field_types{ (\"SalesOrderNumber\": String) })
    CSV(file_name = \"contoso/FactSales.csv\")
    CSV(file_name = \"contoso/FactSalesQuota.csv\")
    CSV(file_name = \"contoso/FactStrategyPlan.csv\")
    ";

    let parse_result = ast_parser(string_to_parse);

    assert!(parse_result.is_ok());

    let mut online_sales_field_types = HashMap::new();
    online_sales_field_types.insert("SalesOrderNumber".to_string(), DataTypeDescriptor::String);

    let mut dim_date_field_types = HashMap::new();
    dim_date_field_types.insert("DateKey".to_string(), DataTypeDescriptor::Date("%Y-%m-%d"));

    let expected_ast = Ast {
        loadable_filenames: vec![
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimAccount.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimChannel.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimCurrency.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimCustomer.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimDate.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimEmployee.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimEntity.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimGeography.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimMachine.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimOutage.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProduct.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProductCategory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimProductSubcategory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimPromotion.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimSalesTerritory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimScenario.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/DimStore.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactExchangeRate.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactInventory.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactITMachine.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactITSLA.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactOnlineSales.csv".to_string(),
                separator: None,
                field_types: Some(online_sales_field_types),
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactSales.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactSalesQuota.csv".to_string(),
                separator: None,
                field_types: None,
            }),
            LoadableFormatData::CSV(CSVData {
                filename: "contoso/FactStrategyPlan.csv".to_string(),
                separator: None,
                field_types: None,
            }),
        ],
    };

    assert_eq!(parse_result, Ok(("", expected_ast)));

    let (_, ast) = parse_result.unwrap();
    assert!(Context::convert_ast(&ast).is_ok());
}
