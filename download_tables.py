from datetime import date, timedelta
import os
import requests
from timeit import default_timer as timer
import zipfile

# Base url to StatsCan API
base_url = "https://www150.statcan.gc.ca/t1/wds/rest/"

# Download a zip file from a url
def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


# Download table from given product id
def download_table(productId: str):
    # Get changed table url
    url = base_url + f"getFullTableDownloadCSV/{productId}/en"
    response = requests.get(url)
    table_url = response.json()["object"]

    # Download table csv
    download_path = f"data/{productId}.zip"
    download_url(url=table_url, save_path=download_path)

    # Unzip folder
    unzip_path = f"data/{productId}"
    with zipfile.ZipFile(download_path, "r") as zip_ref:
        zip_ref.extractall(unzip_path)

    # Delete zip folder
    os.remove(download_path)


if __name__ == "__main__":
    start = timer()

    # Create data folder
    if not os.path.exists("data/"):
        os.makedirs("data/")

    # Get changed tables list
    current_date = str(date.today())
    url = base_url + f"getChangedCubeList/{current_date}"

    response = requests.get(url)
    results = response.json()["object"]

    # Download all tables
    for result in results:
        download_table(result["productId"])

    end = timer()
    elapsed_time = timedelta(seconds=round(end - start))
    print(f"End of scraping animelists elapsed time of {elapsed_time}")
