import json
from concurrent.futures import ThreadPoolExecutor
from logging import INFO, basicConfig, getLogger
from os import cpu_count, mkdir
from shutil import copyfile
from subprocess import check_call, check_output
from tempfile import TemporaryDirectory

from modules import (
    create_cog,
    get_image,
    median_composite,
    reproject_roi,
)

from .config import END_DATE, OUTPUT_PREFIX, RESOLUTION, ROI, START_DATE

# Configuration
MAX_WORKERS = cpu_count() or 1  # Thread workers
GDAL_NUM_THREADS = "ALL_CPUS"

collection = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"

basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = getLogger(__name__)

# Temp folder
temp_folder = TemporaryDirectory(delete=False).name

# Bands size
bands_size = 64

# Convert geojson to EPSG:4326
logger.info("Loading ROI")
roi_reproject, crs_roi, bounds, shape = reproject_roi(ROI, RESOLUTION, temp_folder)

date_prop = "startTime"

# search earth engine data
features = json.loads(
    check_output(
        f"""ogrinfo \
            -features \
            -json \
            -spat {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
            -where "startTime >= '{START_DATE.replace("-", "/")} 00:00:00' AND endTime <= '{END_DATE.replace("-", "/")} 23:59:59'" \
            "EEDA:projects/earthengine-public/assets/{collection}"
        """,
        shell=True,
        text=True,
    )
)["layers"][0]["features"]

# Get unique dates
date_list = list(
    set(
        [
            feat["properties"][date_prop].split(" ")[0].replace("/", ".")
            for feat in features
        ]
    )
)
date_list.sort()
logger.info(f"Date count: {len(date_list)}")


# Process each date in parallel
def process_date(
    date_str: str,
) -> list[str]:
    """Process all bands for a single date"""
    logger.info(f"Processing date {date_str}")

    features_date = list(
        filter(
            lambda f: f["properties"][date_prop].split(" ")[0].replace("/", ".")
            == date_str,
            features,
        )
    )

    date_str = date_str.split(" ")[0].replace("/", "-")

    logger.info(f"Downloading {date_str} from Earth Engine")

    # Function to process per band
    def processing_band(b=int):
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            # Process per band
            image_paths_jobs = [
                executor.submit(
                    get_image,
                    image_path=features_date[y]["properties"]["gdal_dataset"],
                    bounds=bounds,
                    shape=shape,
                    crs=crs_roi,
                    image_name=f"cut_{date_str}_{y}_band_{b}",
                    temp_folder=temp_folder,
                    oo=f"-oo PIXEL_ENCODING=GEO_TIFF -oo BANDS=A{b:02d}",
                    dtype="Float32",
                    nodata="NaN",
                )
                for y in range(len(features_date))
            ]

            image_paths = [job.result() for job in image_paths_jobs]

        # Create band VRT
        image_list_text = f"{temp_folder}/list_{date_str}_band_{b}.txt"
        with open(image_list_text, "w") as file:
            file.write("\n".join(image_paths))

        vrt_url = f"{temp_folder}/vrt_{date_str}_band_{b}.vrt"
        check_call(
            f"gdalbuildvrt -input_file_list {image_list_text} -overwrite {vrt_url}",
            shell=True,
        )

        # Process band image
        logger.info(f"Mosaic band {b} {date_str}")
        cut_data = get_image(
            image_path=vrt_url,
            bounds=bounds,
            shape=shape,
            crs=crs_roi,
            image_name=f"cut_{date_str}_band_{b}",
            temp_folder=temp_folder,
            nodata="-9999",
            dtype="Float32",
        )

        # Rescale data
        logger.info(f"Rescale band {b} {date_str}")
        rescale = f"{temp_folder}/rescale_{date_str}_band_{b}.tif"
        check_call(
            f"""gdal_calc \
                -A {cut_data} \
                --outfile={rescale} \
                --calc="((A != -9999) * ((A + 1) * 5000)) + ((A == -9999) * 0)" \
                --type=UInt16 \
                --co="COMPRESS=ZSTD" \
                --NoDataValue=0 \
                --overwrite
          """,
            shell=True,
        )

        # Return to float32
        logger.info(f"Return image to Float32 for nanmedian band {b} {date_str}")
        float_data = get_image(
            image_path=rescale,
            bounds=bounds,
            shape=shape,
            crs=crs_roi,
            image_name=f"float_{date_str}_band_{b}",
            temp_folder=temp_folder,
            nodata="NaN",
            dtype="Float32",
        )

        return float_data

    # Get mosaic per band result
    with ThreadPoolExecutor(8) as executor:
        bands_jobs = [executor.submit(processing_band, b) for b in range(bands_size)]
        bands_results = [job.result() for job in bands_jobs]

    return bands_results


# Loop process per date
date_result = [process_date(date_str) for date_str in date_list]

# Create median composite
with ThreadPoolExecutor(8) as executor:
    # Submit all band processing tasks
    band_median_jobs = [
        executor.submit(
            median_composite,
            image_list=[result[b] for result in date_result],
            formula="nanmedian(A, axis=0)",
            image_name=f"median_band_{b}",
            temp_folder=temp_folder,
        )
        for b in range(bands_size)
    ]

    # Collect results as they complete
    band_median_results = [job.result() for job in band_median_jobs]

# Create VRT with bands in correct order
vrt = f"{temp_folder}/satellite_embedding.vrt"
check_call(
    f"gdalbuildvrt -separate -overwrite {vrt} {' '.join(band_median_results)}",
    shell=True,
)

# Create final COG
logger.info("Creating final COG")
final_path = create_cog(
    image_path=vrt,
    bounds=bounds,
    shape=shape,
    crs=crs_roi,
    data_type="UInt16",
    nodata=0,
    image_name="satellite_embedding_cog",
    temp_folder=temp_folder,
    roi=roi_reproject,
)

# Copy data to output folder
logger.info("Copying to output")
try:
    mkdir(f"output/{OUTPUT_PREFIX}")
except Exception:
    logger.info("Folder already exist")

copyfile(final_path, f"output/{OUTPUT_PREFIX}/satellite_embedding.tif")
logger.info("Data copied")

# Clean folder
check_call(f"rm -rf {temp_folder}", shell=True)
