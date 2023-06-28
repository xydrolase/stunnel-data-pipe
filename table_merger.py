#!/usr/bin/env python3

"""
Job to merge small Arrow IPC files into larger PyArrow tables, and save as Parquet file.
"""

from datetime import date, datetime
from argparse import ArgumentParser
import os.path
import io
from typing import Optional

import boto3
import boto3.s3
from mypy_boto3_s3.service_resource import S3ServiceResource
import pyarrow as pa
import pyarrow.parquet as pq


def parse_args():
    parser = ArgumentParser(description="Merge small Arrow IPC files into larger Parquet files")
    parser.add_argument("--bucket", required=True, help="The name of the S3 bucket")
    parser.add_argument('-s', "--source-dir", default="raw_data",
                        help="The source data directory containing raw data.")
    parser.add_argument("--target-dir", default="processed",
                        help="The target/destionation directory for processed (Parquet) data")
    parser.add_argument("--date", type=date.isoformat,
                        help="The ISO formatted date of data to be processed (e.g. '2023-06-10')")
    parser.add_argument('-p', "--profile", default="default", dest="aws_profile",
                        help="The name of the AWS profile to retrieve credentials.")
    parser.add_argument('-B', "--record-batch-size", default=10000, type=int,
                        help="The preferred record batch size (row group size) to be written to the output Parquet file.")


    args = parser.parse_args()
    if args.date is None:
        args.date = datetime.now().date()

    return args

def load_arrow_table_from_key(s3: S3ServiceResource, bucket: str, key: str):
    obj = s3.Object(bucket, key) 
    return pa.ipc.open_file(io.BytesIO(obj.get()['Body'].read())).read_all()


class ChunkedParquetWriter:
    def __init__(self, filename, schema, row_group_size):
        self._writer = pq.ParquetWriter(filename, schema)
        self.row_group_size = row_group_size
        self.closed = False

        self._buffered_chunks = []
        self._buffer_size = 0

    def append_table(self, table: pa.Table):
        self._buffer_size += table.num_rows
        self._buffered_chunks.extend(table.to_batches())

        if self._buffer_size > self.row_group_size:
            self._flush()

    def _flush(self):
        assert not self.closed, "Cannot flush to a closed ParquetWriter."

        combined_table = pa.Table.from_batches(self._buffered_chunks).combine_chunks()
        self._writer.write_table(combined_table)

        self._buffer_size = 0
        self._buffered_chunks = []

    def close(self):
        if not self.closed:
            self._flush()
            self._writer.close()
    

def main():
    args = parse_args()
    session = boto3.Session(profile_name=args.aws_profile)
    s3 = session.resource('s3')

    date_str = datetime.strftime(args.date, "%Y/%m/%d")

    bucket = s3.Bucket(args.bucket)
    writer: Optional[ChunkedParquetWriter] = None

    for i, obj in enumerate(bucket.objects.filter(Prefix=os.path.join(args.source_dir, date_str))):
        print(f"Loading {obj.key}")

        table = load_arrow_table_from_key(s3, args.bucket, obj.key)
        if writer is None:
            writer = ChunkedParquetWriter("/tmp/concat.parquet", table.schema, args.record_batch_size)

        writer.append_table(table)

        if i > 10:
          break

    writer.close()

if __name__ == "__main__":
    main()
