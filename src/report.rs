//! The `report` module provides code for working with the elements in the `DataStore` to generate
//! the report.
use crate::data_store::DataStore;
use rust_decimal::Decimal;

/// Create a formatted string representing the record from the `DataStore`.
///
/// # Arguments
///
/// * `difference` - The record's difference value.
/// * `code` - The code representing the record's description.
/// * `data_store` - The `DataStore` instance used to convert the code to the description.
///
/// # Returns
///
/// An Option which will contain the formatted record for the report if the record code
/// could be converted to a description.
fn record_string(difference: &Decimal, code: &usize, data_store: &DataStore) -> Option<String> {
    if let Some(description) = data_store.get_description_for_code(*code) {
        if difference.is_zero() || difference.is_sign_positive() {
            Some(format!("${}: {}\n", difference.round_dp(2), description))
        } else {
            Some(format!(
                "-${}: {}\n",
                difference.abs().round_dp(2),
                description
            ))
        }
    } else {
        None
    }
}

/// Generate the report for the exercise.
///
/// # Arguments
///
/// * `data_store` - The records store.
/// * `count` - The number of records requested for the report.
/// * `year` - The requested year for the report.
///
/// # Returns
///
/// A new String containing the report.
pub fn generate_report(data_store: &DataStore, count: &usize, year: &i32) -> String {
    let mut report = format!("Top {count} NADAC per unit price increases of {year}:\n");
    for record in data_store.get_top().iter().rev() {
        if let Some(record_str) = record_string(record.0, record.1, data_store) {
            report.push_str(&record_str);
        }
    }

    report.push_str("\n");

    report.push_str(&format!(
        "Top {count} NADAC per unit price decreases of {year}:\n"
    ));

    for record in data_store.get_bottom().iter() {
        if let Some(record_str) = record_string(record.0, record.1, data_store) {
            report.push_str(&record_str);
        }
    }

    report
}
