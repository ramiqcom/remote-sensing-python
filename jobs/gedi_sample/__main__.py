import json
import os
from concurrent.futures import ThreadPoolExecutor
from logging import INFO, basicConfig, getLogger
from shutil import copyfile
from subprocess import check_call, check_output
from tempfile import NamedTemporaryFile, TemporaryDirectory

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from modules import get_image, reproject_roi
from shapely.geometry import Point

from .config import END_DATE, GEDI_IDS, OUTPUT_PREFIX, PARAMETERS, ROI, START_DATE

basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = getLogger(__name__)

# Number of workers for parallel processing
MAX_WORKERS = os.cpu_count() or 1

# Temp folder
temp_folder = TemporaryDirectory(delete=False)

# Load the ROI
logger.info("Loading ROI")
roi, crs_roi, bounds, shape = reproject_roi(ROI, 25, temp_folder.name)


# function to process per gedi id for GEE
def get_gedi_gee(id) -> None:
    """Process all files for a single GEDI ID."""
    logger.info(f"Processing {id}")

    # Data parameter
    parameters = PARAMETERS[id]
    column = parameters["column"]
    gedi_band = parameters["band"]
    multiplier = parameters["multiplier"]
    quality_flag = parameters["quality_flag"]

    # search earth engine data
    features = json.loads(
        check_output(
            f"""ogrinfo \
                -features \
                -json \
                -spat {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
                -where "startTime >= '{START_DATE.replace("-", "/")} 00:00:00' AND endTime <= '{END_DATE.replace("-", "/")} 23:59:59'" \
                "EEDA:projects/earthengine-public/assets/LARSE/GEDI/{id}_002_MONTHLY"
            """,
            shell=True,
            text=True,
        )
    )["layers"][0]["features"]

    # Process per image
    def process_each_feature(x):
        temp_folder = TemporaryDirectory(delete=False)

        feature = features[x]
        properties = feature["properties"]
        feature_id = properties["id"]
        gdal_dataset = properties["gdal_dataset"]

        logger.info(f"Processing feature {feature_id} {x + 1} / {len(features)}")

        oo = "-oo PIXEL_ENCODING=GEO_TIFF"

        # get degrade flag
        logger.info("Get degrade flag image")
        degrade_flag_image = get_image(
            image_path=gdal_dataset,
            bounds=bounds,
            shape=shape,
            crs=crs_roi,
            image_name="degrade_flag",
            temp_folder=temp_folder.name,
            oo=f"{oo} -oo BANDS=degrade_flag",
            dtype="Byte",
        )

        # get quality flag
        logger.info("Get quality flag image")
        quality_flags_images = [
            get_image(
                image_path=gdal_dataset,
                bounds=bounds,
                shape=shape,
                crs=crs_roi,
                image_name=band_name,
                temp_folder=temp_folder.name,
                oo=f"{oo} -oo BANDS={band_name}",
                dtype="Byte",
            )
            for band_name in quality_flag
        ]

        # get the main image
        logger.info("Get the main image")
        gedi_image = get_image(
            image_path=gdal_dataset,
            bounds=bounds,
            shape=shape,
            crs=crs_roi,
            image_name=gedi_band,
            temp_folder=temp_folder.name,
            oo=f"{oo} -oo BANDS={gedi_band}",
            dtype="Float32",
        )

        # formula for masking and multiplier
        band_param = (
            f"-A {gedi_image} -B {degrade_flag_image} -C {quality_flags_images[0]}"
        )
        calc = f"A * {multiplier} * (B == 0) * (C == 1)"
        if id == "GEDI04_A":
            band_param = f"{band_param} -D {quality_flags_images[1]}"
            calc = f"{calc} * (D == 1)"

        # create only valid data
        logger.info("Calculate valid image")
        valid_image = f"{temp_folder.name}/valid.tif"
        check_call(
            f"""gdal_calc \
            {band_param} \
            --calc="{calc}" \
            --outfile={valid_image} \
            --type=UInt16 \
            --NoDataValue=0 \
            --co="COMPRESS=ZSTD"
        """,
            shell=True,
        )

        # open the data to turn into table
        with rio.open(valid_image) as source:
            logger.info("Read the image")
            raster = source.read(1)  # Read first band
            transform = source.transform  # Get the transformation

            # Create mask for non-zero and non-NaN pixels
            mask = raster != source.nodata

            # Get row, col indices where mask is True
            rows, cols = np.where(mask)

            # Convert to coordinates
            coords = [rio.transform.xy(transform, r, c) for r, c in zip(rows, cols)]

            # Combine into list of (x, y)
            geometries = [
                Point(x, y)
                for x, y in zip([c[0] for c in coords], [c[1] for c in coords])
            ]

            # Get values
            values = raster[rows, cols]

            # Convert to DataFrame and save
            df = gpd.GeoDataFrame(
                [{f"{column}": value} for value in values],
                geometry=geometries,
                crs="EPSG:4326",
            )
            logger.info(df)

        return df

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        jobs = [executor.submit(process_each_feature, x) for x in range(len(features))]
        result = []

        for job in jobs:
            try:
                result.append(job.result())
            except Exception as e:
                logger.info(f"Error: {e.args}")

    # combine all the table
    logger.info("Combine all the table")
    tables = pd.concat(result)

    # Save the data
    with NamedTemporaryFile(suffix=".fgb") as tmp:
        tables.to_file(tmp.name, driver="FlatGeobuf", **{"SPATIAL_INDEX": "YES"})

        # Copy it to output prefix
        logger.info(f"Copying to output {id}")
        try:
            os.mkdir(f"output/{OUTPUT_PREFIX}")
        except Exception:
            logger.info("Folder already exist")
        copyfile(tmp.name, f"output/{OUTPUT_PREFIX}/GEDI_{id}.fgb")

        logger.info("Copying success")


# Process each GEDI ID in sequence (since earthaccess might have rate limits)
with ThreadPoolExecutor(MAX_WORKERS) as executor:
    jobs = []

    for id in GEDI_IDS:
        jobs.append(executor.submit(get_gedi_gee(id)))

    for job in jobs:
        try:
            job.result()
        except Exception as e:
            logger.info(f"Error: {e.args}")

# Clean up
temp_folder.cleanup()
