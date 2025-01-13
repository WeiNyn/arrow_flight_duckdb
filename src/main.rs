use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::{FlightData, Ticket};
use polars::prelude::*;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the Flight server
    let mut client = FlightServiceClient::connect("http://localhost:8815").await?;

    // Create a ticket to request data
    let ticket = Ticket::new("SELECT * FROM ABC");

    // Request data from the server
    let mut stream = client.do_get(ticket).await?.into_inner();

    // Create a temporary file to store the data
    let mut temp_file = NamedTempFile::new()?;
    let temp_file_path = temp_file.path().to_str().unwrap().to_string();

    // Initialize the IPC writer
    let writer: Option<ipc::writer::FileWriter<&mut NamedTempFile>> = None;

    while let Some(flight_data) = stream.next().await {
        match flight_data {
            Ok(flight_data) => {
                let batch = flight_data_to_batches(&[flight_data])?;
                if writer.is_none() {
                    writer = Some(
                        ipc::writer::FileWriter::try_new(&mut temp_file, &batch[0].schema())
                            .unwrap(),
                    );
                }
                if let Some(ref mut writer) = writer {
                    writer.write(&batch[0])?;
                }
            }
            Err(e) => {
                eprintln!("Error receiving flight data: {:?}", e);
            }
        }
    }

    // Finalize the writer
    if let Some(writer) = writer {
        writer.finish()?;
    }

    // Use the temporary file with Polars
    let df = LazyFrame::scan_ipc(temp_file_path, Default::default())?;
    println!("{:?}", df.collect()?);

    Ok(())
}
