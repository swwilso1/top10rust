//! The `record_pool` module provides code for storing difference/descriptions for a
//! range of records.

use rust_decimal::Decimal;
use std::collections::HashMap;

/// Enum that controls the accounting of the ordering of the elements
/// in a `RecordPool`.
#[derive(Debug)]
pub enum PoolType {
    /// The `RecordPool` contains the top largest values.
    Most,

    /// The `RecordPool` contains the bottom smallest values.
    Least,
}

/// The `RecordPool` has a container for the difference/description codes and
/// the other elements needed to efficiently insert and track the pool records.
/// The `RecordPool` is designed to work closely with the `DataStore`.
#[derive(Debug)]
pub struct RecordPool {
    /// The map of the difference values and their corresponding description code.
    pub records: HashMap<Decimal, usize>,

    /// The largest difference stored in the pool.
    pub largest: Decimal,

    /// The smallest difference stored in the pool.
    pub smallest: Decimal,

    /// The number of records allowed in the pool.
    pub bounds: usize,

    /// The type of pool, either tracking the biggest values or the smallest values.
    pub pool_type: PoolType,
}

impl RecordPool {
    /// Create a new pool.
    ///
    /// # Arguments
    ///
    /// * `bounds` - The number of records allowed in the pool.
    /// * `pool_type` - The behavior type of the pool.
    pub fn new(bounds: usize, pool_type: PoolType) -> Result<RecordPool, String> {
        if bounds == 0 {
            return Err("Bounds for RecordPool cannot be 0".to_string());
        }

        Ok(RecordPool {
            records: HashMap::new(),
            largest: Decimal::new(0, 0),
            smallest: Decimal::new(0, 0),
            bounds,
            pool_type,
        })
    }

    /// Determine if the argument difference value should be a member of the pool.
    ///
    /// # Argument
    ///
    /// * `difference` - The price difference calculated from a CSV record.
    ///
    /// # Returns
    ///
    /// Returns true if the pool has fewer records than its upper bound or if the difference
    /// is in the range [lowest, highest] for the pool.
    pub fn fits(&self, difference: &Decimal) -> bool {
        // If we do not have enough records in the pool yet, then it fits!
        if self.records.len() < self.bounds {
            return true;
        }

        match self.pool_type {
            PoolType::Most => {
                // In the pool where we track the most, if the difference is bigger than the largest
                // element it fits.
                if *difference > self.largest {
                    return true;
                }
            }
            PoolType::Least => {
                // In the pool where we track the least, if the difference is smaller than the smallest
                // difference, it fits.
                if *difference < self.smallest {
                    return true;
                }
            }
        }

        // Now test to see if the difference is in the range [smallest, largest]
        if *difference >= self.smallest && *difference <= self.largest {
            return true;
        }

        false
    }

    /// Insert a difference/code into the pool.
    ///
    /// # Arguments
    ///
    /// * `difference` - The difference value computed from a CSV record.
    /// * `code` - The code representing the CSV record's description.
    ///
    /// # Returns
    ///
    /// If the function successfully inserts the difference/code, and it replaces
    /// a difference/code already in the pool, the function will return a tuple
    /// containing the replaced value.
    pub fn insert(
        &mut self,
        difference: Decimal,
        description_code: usize,
    ) -> Option<(Decimal, usize)> {
        // Check to see if the difference fits and that we do not already have this difference
        // in the pool.
        if self.fits(&difference) {
            // See if we already have this difference/code in the pool. If so, then just jump
            // out of this function so we do not insert duplicate records.
            if let Some(code) = self.records.get(&difference) {
                if description_code == *code {
                    return None;
                }
            }

            self.records.insert(difference, description_code);

            // Check to see if we have exceeded the allowed number of records in the pool.
            if self.records.len() > self.bounds {
                // We have inserted a new difference value which means our cached smallest/largest
                // values are invalid. Get the keys of the differences and use the keys array to
                // calculate what we need to remove.
                let mut keys: Vec<Decimal> = self.records.keys().map(|k| k.clone()).collect();

                // Sort the keys so that smallest is in keys.first and largest is in keys.last.
                keys.sort();
                let result = match self.pool_type {
                    PoolType::Most => {
                        // We already know that we have more than one key because the number
                        // of records in the map exceed our bounds. Even if bounds is 0 that
                        // means we have at least one key. Similarly, that key has a value.
                        // Thus, we can safely unwrap the results of a get operation.
                        let key = keys.remove(0);
                        let value = self.records.get(&key).unwrap();
                        let result = Some((key.clone(), *value));
                        self.records.remove(&key);
                        result
                    }
                    PoolType::Least => {
                        let key = keys.pop().unwrap();
                        // Similarly, this unwrap is safe.
                        let value = self.records.get(&key).unwrap();
                        let result = Some((key.clone(), *value));
                        drop(keys);
                        self.records.remove(&key);
                        result
                    }
                };
                // We have now removed the excess item, so recalculate the keys with a sort
                // to get the smallest and largest.
                let mut keys: Vec<&Decimal> = self.records.keys().collect();
                keys.sort();
                // Since we are
                self.smallest = (*keys.first().unwrap()).clone();
                self.largest = (*keys.last().unwrap()).clone();
                result
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Return an iterator capable of iterating through the pool in the correct order
    /// depending on whether the pool is of type least or most.
    pub fn iter(&self) -> RecordPoolIterator {
        RecordPoolIterator::new(self)
    }
}

/// Create a simple iterator struct that can track the elements in
/// the pool.
#[derive(Debug)]
pub struct RecordPoolIterator<'a> {
    /// The pool reference.
    pool: &'a RecordPool,

    /// The keys for the elements in the pool. Caching them here
    /// only in the iterator helps to do the correct in-order
    /// traversal of the elements without keeping them as a copy
    /// in the pool itself.
    keys: Vec<&'a Decimal>,

    /// For forward iteration, use the index
    index: usize,

    /// Used for reverse iteration. We start at the end and decrement the first
    /// element of the tuple. When the element is 0, and we have iterated it,
    /// then set the sentinel boolean to false indicating the end-of-iteration.
    rindex: (usize, bool),
}

impl<'a> RecordPoolIterator<'a> {
    /// Create a new iterator.
    ///
    /// # Arguments
    ///
    /// * `pool` - The pool to which the iterator refers.
    pub fn new(pool: &'a RecordPool) -> RecordPoolIterator<'a> {
        // If the pool is empty then we are at the end of the reverse
        // iterator. Otherwise, set it up correctly for walking backwards
        // through the values.
        let rindex = if pool.records.is_empty() {
            (0, true)
        } else {
            (pool.records.len() - 1, false)
        };

        let mut keys: Vec<&Decimal> = pool.records.keys().collect();
        keys.sort();

        RecordPoolIterator {
            pool,
            keys,
            index: 0,
            rindex,
        }
    }
}

/// Iterator implementation provided for the pool iterator.
impl<'a> Iterator for RecordPoolIterator<'a> {
    type Item = (&'a Decimal, &'a usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.pool.records.len() {
            let key = &self.keys[self.index];
            // This get call is valid as long as the keys are borrowed
            // from the pool.
            let value = self.pool.records.get(*key).unwrap();
            self.index += 1;
            Some((key, value))
        } else {
            None
        }
    }
}

/// Provided DoubleEndedIterator trait implementation so we can do
/// for record in record_pool.iter().rev() {}
impl<'a> DoubleEndedIterator for RecordPoolIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.rindex.1 {
            None
        } else {
            let key = &self.keys[self.rindex.0];
            // This get call is valid as long as the keys are borrowed
            // from the pool.
            let value = self.pool.records.get(key).unwrap();
            if self.rindex.0 == 0 {
                self.rindex.1 = true;
            } else {
                self.rindex.0 -= 1;
            }
            Some((*key, value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_with_most_pool() {
        let mut pool = RecordPool::new(3, PoolType::Most).unwrap();

        let d1 = Decimal::new(1, 0);
        let d2 = Decimal::new(2, 0);
        let d3 = Decimal::new(3, 0);
        let d4 = Decimal::new(4, 0);
        let d5 = Decimal::new(5, 0);
        let d6 = Decimal::new(32, 1);

        pool.insert(d1.clone(), 1);
        pool.insert(d2.clone(), 2);
        pool.insert(d3.clone(), 3);
        pool.insert(d4.clone(), 4);
        pool.insert(d5.clone(), 5);
        pool.insert(d6.clone(), 6);

        assert_eq!(pool.records.len(), 3);

        let mut counter = 0;
        for record in pool.iter() {
            if counter == 0 {
                assert_eq!(*record.0, d6);
            } else if counter == 1 {
                assert_eq!(*record.0, d4);
            } else if counter == 2 {
                assert_eq!(*record.0, d5);
            }

            counter += 1;
        }
    }

    #[test]
    fn test_insert_with_least_pool() {
        let mut pool = RecordPool::new(3, PoolType::Least).unwrap();

        let d1 = Decimal::new(-1, 0);
        let d2 = Decimal::new(-2, 0);
        let d3 = Decimal::new(-3, 0);
        let d4 = Decimal::new(-4, 0);
        let d5 = Decimal::new(-5, 0);
        let d6 = Decimal::new(-32, 1);

        pool.insert(d1.clone(), 1);
        pool.insert(d2.clone(), 2);
        pool.insert(d3.clone(), 3);
        pool.insert(d4.clone(), 4);
        pool.insert(d5.clone(), 5);
        pool.insert(d6.clone(), 6);

        assert_eq!(pool.records.len(), 3);

        let mut counter = 0;
        for record in pool.iter().rev() {
            if counter == 0 {
                assert_eq!(*record.0, d6);
            } else if counter == 1 {
                assert_eq!(*record.0, d4);
            } else if counter == 2 {
                assert_eq!(*record.0, d5);
            }

            counter += 1;
        }
    }
}
