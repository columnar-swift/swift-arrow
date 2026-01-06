# Useful for debugging IPC writing issues.
import pyarrow as pa
import sys

print(f"PyArrow version: {pa.__version__}")

try:
    with open(sys.argv[1], 'rb') as f:
        reader = pa.ipc.open_file(f)
        print(f"Schema: {reader.schema}")
        print(f"Num batches: {reader.num_record_batches}")
        
        for i in range(reader.num_record_batches):
            batch = reader.get_batch(i)
            print(f"Batch {i}: {batch.num_rows} rows, {batch.num_columns} columns")
        
        print("✓ File read successfully")
except Exception as e:
    print(f"✗ Error: {e}")
    sys.exit(1)
