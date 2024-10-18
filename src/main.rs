mod data_store;
mod record_pool;
mod report;

use crate::report::generate_report;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};

static NADAC_COMPARISON_URL: &str =
    "https://download.medicaid.gov/data/nadac-comparison-04-17-2024.csv";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // Price change data URL
    #[arg(
        short,
        long,
        default_value = NADAC_COMPARISON_URL
    )]
    url: String,

    // Number of top per-unit price increases and decreases
    #[arg(short, long, default_value_t = 10)]
    count: usize,

    // Drug price change year to report on
    #[arg(short, long, default_value_t = 2023)]
    year: i32,
}

const EFFECTIVE_DATE_FIELD: usize = 9;

async fn generate_nadac_top_price_change_report(
    url: &str,
    year: i32,
    count: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    // The tricky part here is to convert the stream from the reqwest crate into a stream something
    // that implements the futures::AsyncRead trait needed by csv_async.

    let stream = reqwest::get(url).await?.bytes_stream();

    let async_read_stream = stream
        .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let mut csv_reader = csv_async::AsyncReader::from_reader(async_read_stream);

    let mut records = csv_reader.records();

    let mut data_store: data_store::DataStore = data_store::DataStore::new(count)?;

    while let Some(record) = records.next().await {
        let record = record?;

        if let Some(effective_date) = record.get(EFFECTIVE_DATE_FIELD) {
            if effective_date.is_empty() {
                continue;
            }

            // We could use a crate like chrono and parse the full date field, but in the interest
            // of time, I have chosen just to manually extract the year value from the data.
            let array: Vec<&str> = effective_date.split("/").collect();
            let record_year = array[2].parse::<i32>()?;

            if record_year == year {
                data_store.insert(&record)?;
            }
        }
    }

    Ok(generate_report(&data_store, &count, &year))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let report = generate_nadac_top_price_change_report(&args.url, args.year, args.count).await?;

    print!("{}", report);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{generate_nadac_top_price_change_report, NADAC_COMPARISON_URL};
    use std::path::PathBuf;
    use tokio::io::AsyncReadExt;

    // Since this is test code, I have left the use of unwrap() to explicitly allow
    // the test system to catch panics.
    #[tokio::test]
    async fn test_report() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR").to_string());
        path.push("data");
        path.push("top_10_2020.txt");

        let file = tokio::fs::File::open(path).await.unwrap();
        let mut reader = tokio::io::BufReader::new(file);

        let mut contents: Vec<u8> = Vec::new();
        let _ = reader.read_to_end(&mut contents).await.unwrap();

        let data_report = String::from_utf8_lossy(&contents);

        let generated_report =
            generate_nadac_top_price_change_report(NADAC_COMPARISON_URL, 2020, 10)
                .await
                .unwrap();

        assert_eq!(data_report, generated_report);
    }
}
