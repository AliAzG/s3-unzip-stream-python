import boto3
from stream_unzip import stream_unzip
import httpx
from io import BytesIO

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define your S3 bucket name
bucket_name = 'bucket_name'

def zipped_chunks():
    # Iterable that yields the bytes of a zip file
    with httpx.stream('GET', 'https://bucket.s3.amazonaws.com/path/file.zip') as r:
        yield from r.iter_bytes(chunk_size=65536)

for file_name, file_size, unzipped_chunks in stream_unzip(zipped_chunks()):
    # Define the key (path) where you want to save the file in the S3 bucket
    s3_key = f'unzipped/{file_name}'.replace("'","")
    
    # Create an in-memory buffer to accumulate the chunks
    buffer = BytesIO()
    
    # Iterate through the unzipped chunks and write them to the buffer
    for chunk in unzipped_chunks:
        buffer.write(chunk)
    
    # Reset buffer position to the beginning
    buffer.seek(0)
    
    # Upload the buffer content to the S3 bucket
    s3_client.upload_fileobj(buffer, bucket_name, s3_key)
    
    # Close the buffer
    buffer.close()
    
    print(f"File '{file_name}' has been uploaded to S3 bucket '{bucket_name}' with key '{s3_key}'")
