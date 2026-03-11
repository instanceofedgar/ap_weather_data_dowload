import os
import io
import zipfile
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

TARGET_EXTENSIONS = (".epw", ".ddy", ".stat")
OUTPUT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "weather-files")

def download_all_weather_files(max_workers: int = 10):
    base_url = "https://climate.onebuilding.org/WMO_Region_4_North_and_Central_America/"

    output_dir = OUTPUT_DIRECTORY
    os.makedirs(output_dir, exist_ok=True)

    countries = ["CAN", "USA"]
    url_key = {"CAN": "CAN_Canada", "USA": "USA_United_States_of_America"}

    jobs = []

    for country in countries:
        url = f"{base_url}{url_key[country]}/index.html"

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch index for {country}: status code {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        zip_links = [
            link.get("href")
            for link in soup.find_all("a")
            if link.has_attr("href")
            and link.get("href").endswith("_TMYx.2009-2023.zip")
            and f"{country}_" in link.get("href")
            and ".AP." in link.get("href")  # airports only
        ]

        if not zip_links:
            print(f"No weather zip files found for {country}")
            continue

        print(f"Found {len(zip_links)} airport zip files for {country}")

        for href in zip_links:
            zip_url = f"{base_url}{url_key[country]}/{href}"
            base_name = os.path.basename(href).replace(".zip", "")
            jobs.append((zip_url, base_name))

    # skip only if ALL target files already exist
    pending = [
        (zip_url, base_name)
        for zip_url, base_name in jobs
        if not all(os.path.exists(os.path.join(output_dir, base_name + ext)) for ext in TARGET_EXTENSIONS)
    ]
    skipped = len(jobs) - len(pending)
    print(f"\nSkipping {skipped} already downloaded. Downloading {len(pending)} zips with {max_workers} workers...\n")

    all_downloaded = []

    def download_set(args):
        zip_url, base_name = args
        files = get_files_from_zip_url(zip_url)  # dict of {ext: content}
        if not files:
            return [], zip_url
        saved = []
        for ext, content in files.items():
            out_path = os.path.join(output_dir, base_name + ext)
            with open(out_path, "wb") as f:
                f.write(content)
            saved.append(out_path)
        return saved, zip_url

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_set, job): job for job in pending}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                saved, zip_url = future.result()
                if saved:
                    all_downloaded.extend(saved)
                    print(
                        f"[{i}/{len(pending)}] Saved {len(saved)} files for {os.path.basename(zip_url).replace('.zip', '')}"
                    )
                else:
                    print(f"[{i}/{len(pending)}] Failed: {zip_url}")
            except Exception as e:
                print(f"[{i}/{len(pending)}] Error: {e}")

    print(f"\nTotal downloaded: {len(all_downloaded)} files to '{output_dir}/'")
    return all_downloaded


def get_files_from_zip_url(zip_url: str) -> dict[str, bytes] | None:
    response = requests.get(zip_url)
    if response.status_code != 200:
        print(f"  Failed to download zip: status code {response.status_code}")
        return None

    files = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for name in zf.namelist():
            for ext in TARGET_EXTENSIONS:
                if name.endswith(ext):
                    with zf.open(name) as f:
                        files[ext] = f.read()  # raw bytes, no decode

    return files if files else None


if __name__ == "__main__":
    download_all_weather_files()
