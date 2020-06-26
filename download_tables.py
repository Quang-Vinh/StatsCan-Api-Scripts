import click
from datetime import date, timedelta
import os
import re
import requests
from timeit import default_timer as timer
import zipfile

import daaas_storage


dirname = os.path.dirname(__file__)

# Base url to StatsCan API
base_url = "https://www150.statcan.gc.ca/t1/wds/rest/"


def download_url(url, save_path, chunk_size=128):
    """
    Download a zip file from a url
    
    Arguments:
        url {str} -- Url to download from
        save_path {str} -- Path to save file
        chunk_size -- Chunk size when writing
    """
    r = requests.get(url, stream=True)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def download_table(productId: str, dir_path: str):
    """
    Download a public StatsCan table from given product ID
    
    Arguments: 
        productID {str} -- Product ID of the STC table
        dir_path {str} -- Directory to download tables to
    """
    # Get changed table url
    url = base_url + f"getFullTableDownloadCSV/{productId}/en"
    response = requests.get(url)
    table_url = response.json()["object"]

    # Download table csv
    download_path = f"{dir_path}/{productId}.zip"
    download_url(url=table_url, save_path=download_path)

    # Unzip folder
    unzip_path = f"{dir_path}/{productId}"
    with zipfile.ZipFile(download_path, "r") as zip_ref:
        zip_ref.extractall(unzip_path)

    # Delete zip folder
    os.remove(download_path)


def copy_directory_to_minio(directory: str, storage, bucket):
    """
    Recursively copies csv files in a directory to a minIO bucket
    
    Arguments:
        directory {str} -- Directory path to copy
        bucket -- MinIO bucket object
        storage -- Connection to MinIO client
    """
    csv_pattern = re.compile(r".csv$")
    dir_pattern = re.compile(r"^[^\.]*$")

    for file in os.listdir(directory):
        file_path = f"{directory}/{file}"

        # If it is a directory then copy recurse
        if re.search(dir_pattern, file):
            copy_directory_to_minio(file_path, storage, bucket)
        # Base case if it is csv file then copy to bucket
        elif re.search(csv_pattern, file):
            storage.fput_object(bucket, file_path, file_path)


@click.command()
@click.option(
    "--minio_bucket", is_flag=False, help="Upload downloaded tables to MinIO bucket"
)
def main(minio_bucket):
    start = timer()

    # Create data folder
    data_path = os.path.join(dirname, "data")
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    # Get changed tables list
    current_date = str(date.today())
    url = base_url + f"getChangedCubeList/{current_date}"

    response = requests.get(url)
    results = response.json()["object"]

    # Download all tables
    for result in results:
        download_table(result["productId"], data_path)

    end = timer()
    elapsed_time = timedelta(seconds=round(end - start))
    print(f"End of downloading tables elapsed time of {elapsed_time}")

    # If minio bucket is specified then also upload to that bucket
    if minio_bucket:
        start = timer()

        # Setup minIO bucket
        storage = daaas_storage.get_minimal_client()
        bucket = minio_bucket

        # If the bucket does not follow the convention, this will throw an AccessDenied exception.
        if not storage.bucket_exists(bucket):
            storage.make_bucket(bucket, storage._region)
            print(f"Created bucket: {bucket}")
        else:
            print("Your bucket already exists. 👍")

        # Copy all csv files
        copy_directory_to_minio(data_path, storage, bucket)

        end = timer()
        elapsed_time = timedelta(seconds=round(end - start))
        print(f"End of uploading data to minIO bucket elapsed time of {elapsed_time}")

    return


if __name__ == "__main__":
    main()
