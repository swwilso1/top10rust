//! The `DataStore` module provides code for efficiently caching records from the CSV file.

use crate::record_pool::{PoolType, RecordPool};
use bimap::BiMap;
use csv_async::StringRecord;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

const START_PRICE_INDEX: usize = 2;
const END_PRICE_INDEX: usize = 3;
const DESCRIPTION_INDEX: usize = 0;

/// The `DataStore` provides a place to store records according to the criteria
/// of the assignment:
///
/// - The top N per-unit price changes are recorded.
/// - The bottom N per-unit price changes are recorded.
/// - N for the purposes of the exercise is 10, but we use can any size.
/// - The store is memory efficient (it only stores one copy of the record
///   descriptions).
/// - The store is time efficient.
#[derive(Debug)]
pub struct DataStore {
    /// The pool of records that hold the largest positive price changes.
    pub top: RecordPool,

    /// The pool of records that holds the largest decrease in price changes.
    pub bottom: RecordPool,

    /// A map that efficiently stores just one copy of the record descriptions
    /// for the records in `top` and `bottom`.
    pub descriptions: BiMap<String, usize>,

    /// A small secondary map that helps manage the codes used to map the
    /// records.
    pub code_use: HashMap<usize, usize>,

    /// The next code value to use when mapping a unique record description.
    pub next_code: usize,
}

impl DataStore {
    /// Create a new `DataStore` that will track the top and bottom N price changes
    /// in the CSV data.
    pub fn new(size: usize) -> Result<DataStore, Box<dyn std::error::Error>> {
        Ok(DataStore {
            top: RecordPool::new(size, PoolType::Most)?,
            bottom: RecordPool::new(size, PoolType::Least)?,
            descriptions: BiMap::new(),
            code_use: HashMap::new(),
            next_code: 0,
        })
    }

    /// Insert a record into the data store.
    ///
    /// # Arguments
    ///
    /// * `record` - The CSV record from csv_async.
    ///
    /// # Returns
    ///
    /// On success, returns (), on error returns a std::error::Error in a Box.
    pub fn insert(&mut self, record: &StringRecord) -> Result<(), Box<dyn std::error::Error>> {
        // Get the start and end prices. Convert them to Decimals

        let start_price = match record.get(START_PRICE_INDEX) {
            Some(price) => Decimal::from_str(price)?,
            None => return Err("Failed to get start price".into()),
        };

        let new_price = match record.get(END_PRICE_INDEX) {
            Some(price) => Decimal::from_str(price)?,
            None => return Err("Failed to get new price".into()),
        };

        let description = match record.get(DESCRIPTION_INDEX) {
            Some(code) => code,
            None => return Err("Failed to get description code".into()),
        };

        // Let the rust_decimal crate handle the floating point calculations.
        let difference = new_price - start_price;

        // Check to see if the difference for this record will 'fit' in the top record pool. Here,
        // fit means that either the pool has fewer records than its max capacity or that this
        // difference value is in the range [lowest, highest] (inclusive) for the values already
        // in the pool.
        if self.top.fits(&difference) {
            // The difference should be recorded.  Now either retrieve the record description code
            // or generate a new code (by storing the new description).
            let code = self.code_for_description(description);

            // Now insert the difference and the description code into the top pool. The top pool
            // might return a value (as a Some()) for any value that it kicks out of the pool
            // as a result of the insert operation.
            if let Some((replaced_diff, replaced_code)) = self.top.insert(difference, code) {
                // The top pool kicked out a value, we need to check to see if the value can
                // fit in the bottom pool.
                if self.bottom.fits(&replaced_diff) {
                    self.bottom.insert(replaced_diff, replaced_code);
                } else {
                    // The value didn't fit in the bottom pool so clean up the description codes/
                    // stored descriptions. We removed a value from a pool and depending on whether
                    // the description is duplicated between several records, we may need to delete
                    // the description string.
                    self.cleanup_descriptions(replaced_code);
                }
            }

        // The difference didn't fit in the top pool, see if it will go in the bottom.
        } else if self.bottom.fits(&difference) {
            // Similarly to the top case, get the code for the description (maybe adding a new code).
            let code = self.code_for_description(description);

            // Check to see if the insertion returns a record.
            if let Some((replaced_diff, replaced_code)) = self.bottom.insert(difference, code) {
                // The insert returned a record, see if it would fit in the top. It shouldn't fit,
                // but check anyway.
                if self.top.fits(&replaced_diff) {
                    self.top.insert(replaced_diff, replaced_code);
                } else {
                    // Cleanup the description and code if it is unused.
                    self.cleanup_descriptions(replaced_code);
                }
            }
        }

        Ok(())
    }

    /// Return a reference to the top pool
    pub fn get_top(&self) -> &RecordPool {
        &self.top
    }

    /// Return a reference to the bottom pool.
    pub fn get_bottom(&self) -> &RecordPool {
        &self.bottom
    }

    /// Look up the description string for a code value.
    ///
    /// # Arguments
    ///
    /// * `code` - The code for which to search.
    ///
    /// # Returns
    ///
    /// Return an Option that may contain the description string.
    pub fn get_description_for_code(&self, code: usize) -> Option<String> {
        if let Some(description) = self.descriptions.get_by_right(&code) {
            Some(description.clone())
        } else {
            None
        }
    }

    /// Either retrieve an existing code for the description string or create a new one.
    /// If the function creates a new code, insert the description in the map.
    ///
    /// # Arguments
    ///
    /// * `description` - The description string to convert to a code.
    ///
    /// # Returns
    ///
    /// The existing code or newly assigned code.
    fn code_for_description(&mut self, description: &str) -> usize {
        // See if we already have the value in the map.
        if let Some(code) = self.descriptions.get_by_left(description) {
            // The value is in the map, increase the count value for code
            // so we track how many records reference the description.
            if let Some(count) = self.code_use.get_mut(code) {
                *count += 1;
            }
            *code
        } else {
            // The map does not have this description, so insert it.
            let new_code = self.next_code;
            self.next_code += 1;
            self.descriptions.insert(description.to_string(), new_code);
            self.code_use.insert(new_code, 1);
            new_code
        }
    }

    /// Given a code, decrement the refcount and if the count goes to zero,
    /// remove the description and codes from the maps.
    ///
    /// # Arguments
    ///
    /// * `code` - The code to remove/clean up.
    fn cleanup_descriptions(&mut self, code: usize) {
        if let Some(count) = self.code_use.get_mut(&code) {
            *count -= 1;
            if *count == 0 {
                self.descriptions.remove_by_right(&code);
                self.code_use.remove(&code);
            }
        }
    }
}
