#!/usr/bin/env python3

"""
Job to merge small Arrow IPC files into larger PyArrow tables, and save as Parquet file.
"""

from datetime import date, datetime
from argparse import ArgumentParser
import os.path
import io
from typing import Optional
import tempfile

import boto3
import boto3.s3
from mypy_boto3_s3.service_resource import S3ServiceResource, _Bucket
import pyarrow as pa
import pyarrow.parquet as pq


def parse_args():
    parser = ArgumentParser(description="Merge small Arrow IPC files into larger Parquet files")
    parser.add_argument("--bucket", required=True, help="The name of the S3 bucket")
    parser.add_argument('-s', "--source-dir", default="raw_data",
                        help="The source data directory containing raw data.")
    parser.add_argument('-t', "--target-dir", default="processed",
                        help="The target/destionation directory for processed (Parquet) data")
    parser.add_argument('-d', "--date", type=date.fromisoformat,
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


def find_last_process_timestamp(bucket: _Bucket, dir_name: str) -> Optional[int]:
    success_files = sorted(
        bucket.objects.filter(Prefix=os.path.join(dir_name, "SUCCESS_"), Delimiter="/"),
        key=lambda x: -x.last_modified.timestamp())

    if success_files:
        return success_files[0].last_modified.timestamp()
    else:
        return None




class ChunkedParquetWriter:
    def __init__(self, where, row_group_size):
        """
        :param where: Either the path of the output file, or a file-like object. 
        """
        self._writer: Optional[pq.ParquetWriter] = None

        self.row_group_size = row_group_size
        self.closed = False
        self.where = where

        self._buffered_chunks = []
        self._buffer_size = 0

    def append_table(self, table: pa.Table):
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.where, table.schema)

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
        if not self.closed and self._writer:
            self._flush()
            self._writer.close()
    

def main():
    args = parse_args()
    session = boto3.Session(profile_name=args.aws_profile)
    s3 = session.resource('s3')

    date_str = datetime.strftime(args.date, "%Y/%m/%d")

    bucket = s3.Bucket(args.bucket)
    parquet_file = tempfile.NamedTemporaryFile(suffix=".parquet", mode="w+b", delete=False)
    writer = ChunkedParquetWriter(parquet_file, args.record_batch_size)

    output_dir = os.path.join(args.target_dir, date_str)
    last_processed = find_last_process_timestamp(bucket, output_dir)

    # sort all qualified files by last modified time so that the data written to the table are ordered
    data_files = sorted(
        [
            (obj.key, obj.last_modified.timestamp())
            for obj in bucket.objects.filter(Prefix=os.path.join(args.source_dir, date_str))
            if not last_processed or obj.last_modified.timestamp() > last_processed
        ], key=lambda x: x[1])

    for key, _ in data_files:
        print(f"Loading {key}")

        table = load_arrow_table_from_key(s3, args.bucket, key)
        writer.append_table(table)

    writer.close()
    print(f"Closing {parquet_file.name}")
    parquet_file.close()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    merged_key = os.path.join(output_dir, f"merged_{timestamp}.parquet")
    s3.meta.client.upload_file(parquet_file.name, args.bucket, merged_key)
    print(f"File uploaded to S3: s3://{args.bucket}/{merged_key}")

    # create the SUCCESS file with empty bucket
    bucket.put_object(Key=os.path.join(output_dir, f"SUCCESS_{timestamp}"), Body=b'')

    # delete the local file
    # os.unlink(parquet_file.name)

if __name__ == "__main__":
    main()
