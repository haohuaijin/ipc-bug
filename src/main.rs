use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::io::Cursor;

use arrow::ipc::{
    writer::{FileWriter, IpcWriteOptions},
    CompressionType,
};

#[tokio::main]
async fn main() {
    let batch = generate_record_batch(100, 100);
    println!(
        "num_rows: {}, original batch size: {:?}",
        batch.num_rows(),
        batch.get_array_memory_size(),
    );

    let ipc_options = IpcWriteOptions::default();
    let ipc_options = ipc_options
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .unwrap();
    let buf = Vec::new();
    let mut writer = FileWriter::try_new_with_options(buf, &batch.schema(), ipc_options).unwrap();

    writer.write(&batch).unwrap();
    let hits_buf = writer.into_inner().unwrap();

    println!("ipc buffer size: {:?}", hits_buf.len());

    // read from ipc
    let buf = Cursor::new(hits_buf);
    let reader = arrow::ipc::reader::FileReader::try_new(buf, None).unwrap();
    let batches = reader.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>();

    println!(
        "num rows: {}, after decode batch size: {}, batches len: {}",
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum::<usize>(),
        batches.len()
    );
}

pub fn generate_record_batch(columns: usize, rows: usize) -> RecordBatch {
    let mut fields = Vec::with_capacity(columns);
    for i in 0..columns {
        fields.push(Field::new(&format!("column_{}", i), DataType::Utf8, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns);
    for i in 0..columns {
        let column_data: Vec<String> = (0..rows).map(|j| format!("row_{}_col_{}", j, i)).collect();
        let array = StringArray::from(column_data);
        arrays.push(Arc::new(array) as ArrayRef);
    }
    RecordBatch::try_new(schema, arrays).unwrap()
}
