use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::{
    error::Result as ArrowResult, FlightData, Ticket,
};
use tonic::transport::Channel;
use futures::{StreamExt, TryStreamExt};
use polars::prelude::*;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the Flight server
    let mut client = FlightServiceClient::connect("http://localhost:8815").await?;

    // Create a ticket to request data
    let ticket = Ticket::new("SELECT * FROM ABC");

    // Request data from the server
    let stream = client.do_get(ticket).await?.into_inner().map_err(|e| e.into());
    let mut record_batch_stream =
        FlightRecordBatchStream::new_from_flight_data(stream);

    // Create a temporary file to store the data
    let mut temp_file = NamedTempFile::new()?;
    let temp_file_path = temp_file.path().to_str().unwrap().to_string();

    // Initialize the IPC writer
    let mut writer: Option<ipc::writer::FileWriter<&mut NamedTempFile>> = None;

    while let Some(Ok(record_batch)) = record_batch_stream.next().await {
        
                println!("Received {} rows", record_batch.num_rows());
                if writer.is_none() {
                    writer = Some(
                        ipc::writer::FileWriter::try_new(&mut temp_file, &record_batch.schema())
                            .unwrap(),
                    );
                }
                if let Some(ref mut writer) = writer {
                    writer.write(&record_batch)?;
                }
    }

    // Finalize the writer
    if let Some(mut writer) = writer {
        writer.finish()?;
    }

    // Use the temporary file with Polars
    let df = LazyFrame::scan_ipc(temp_file_path, Default::default())?;
    println!("{:?}", df.collect()?);

    Ok(())
}
