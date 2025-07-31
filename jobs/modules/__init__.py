import json
from logging import INFO, basicConfig, getLogger
from subprocess import check_call

import psutil
from dotenv import load_dotenv

memory = int(psutil.virtual_memory().total / 1e9)

load_dotenv()

basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = getLogger(__name__)


def reproject_roi(
    vector_path: str, resolution: float, temp_folder: str
) -> tuple[str, str, tuple[float, float, float, float], tuple[int, int]]:
    """
    Function to reproject region of interest

    Args:
        vector_path: path to the region of intertest. E.g `"/path/roi.geojson"`
        resolution: resolution of output of the image in meter. E.g `10`
        temp_folder: path to temporary folder to save the data. E.g `"/temp_10"`

    Returns:
        tuple: reprojected_path, crs, bounds, and shape
        - reprojected_path: path to the reprojected region of interest. E.g. `"/temp_10/reprojected.geojson"`
        - crs: coordinate reference system. E.g `"EPSG:4326"`
        - bounds: bounding box of the region of interest. E.g `(110, 0, 111, 1)`
        - shape: optimal shape of the image based on the bounding box and resolution. E.g `(1000, 1000)`
    """
    reprojected_path = f"{temp_folder}/reprojected.fgb"
    crs_roi = "EPSG:4326"
    check_call(
        f"""ogr2ogr \
           -makevalid \
           -explodecollections \
           -t_srs {crs_roi} \
           {reprojected_path} \
           {vector_path}
    """,
        shell=True,
    )

    roi_info = f"{temp_folder}/info.json"
    check_call(
        f"""ogrinfo \
            -json \
            -so \
            {reprojected_path} \
            > {roi_info}
    """,
        shell=True,
        text=True,
    )

    with open(roi_info) as file:
        bounds = tuple(json.load(file)["layers"][0]["geometryFields"][0]["extent"])

    # Defined images optimal shape in 30 meter
    # 1 degree = 111.000 meter
    logger.info("Creating optimal raster shape")
    width = int(abs(bounds[0] - bounds[2]) * 111_000 / resolution)
    height = int(abs(bounds[1] - bounds[3]) * 111_000 / resolution)
    shape = (height, width)
    logger.info(f"Optimal Shapes: {shape}")

    return reprojected_path, crs_roi, bounds, shape


def get_image(
    image_path: str,
    bounds: tuple[float, float, float, float],
    shape: tuple[int, int],
    crs: str,
    image_name: str,
    temp_folder: str,
    bands: list[int] | None = None,
    nodata: str | float | None = None,
    dtype: str | None = None,
    oo: str | None = None,
) -> str:
    """
    Function to get and clip image

    Args:
        image_path: path to the image. E.g `"/temp/image.tif"`
        bounds: bounding box of the region of interest. E.g `(110, 0, 111, 1)`
        shape: optimal shape of the image based on the bounding box and resolution. E.g `(1000, 1000)`
        crs: coordinate reference system. E.g `"EPSG:4326"`
        image_name: name of the image. E.g `"cool_image"`
        temp_folder: temporary folder to put the image. E.g `"/temp_x"`
        bands: list of bands to take. E.g [1]

    Returns:
        str: path to the output image. E.g `"/temp_x/cool_image.tif"`
    """
    # output location
    image = f"{temp_folder}/{image_name}.tif"

    # bands parameter
    bands_param = ""
    if type(bands) is list:
        for b in bands:
            bands_param = f"{bands_param}-b {b} "

    # run command
    check_call(
        f"""gdalwarp \
            -ts {shape[1]} {shape[0]} \
            -te {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
            -t_srs {crs} \
            {f"-ot {dtype}" if dtype is not None else ""} \
            {f"-dstnodata {nodata}" if nodata is not None else ""} \
            {bands_param} \
            -co COMPRESS=ZSTD \
            -overwrite \
            -wm {memory}G \
            -wo NUM_THREADS=ALL_CPUS \
            -multi \
            {oo if oo is not None else ""} \
            "{image_path}" \
            {image}
    """,
        shell=True,
    )

    return image


def masking_image(
    image_map: dict[str, str],
    formula: str,
    image_name: str,
    temp_folder: str,
    all_bands: str | None = None,
    dtype: str | None = "UInt16",
) -> str:
    """
    Function to mask the image

    Args:
        image_map: dictionary of key and image path. E.g `dict(A="/temp/image_a.tif", B="/temp/image_b.tif")`
        formula: formula of the masking. E.g `"A*(B>1)"`
        image_name: name of the output image. E.g `"masked_image"`
        temp_folder: path to temporary folder to save the image. E.g `"/temp_1"`

    Returns:
        str: path to the output image. E.g `/temp_1/masked_image.tif`
    """
    cloud_mask = f"{temp_folder}/{image_name}.tif"
    map_param = ""
    for key in image_map.keys():
        map_param = f"{map_param}-{key} {image_map[key]} "

    all_bands_param = ""
    if all_bands is not None:
        all_bands_param = f"--allBands={all_bands}"

    check_call(
        f"""gdal_calc \
           {map_param} \
            --outfile={cloud_mask} \
            --calc="{formula}" \
            --NoDataValue=0 \
            --co="COMPRESS=ZSTD" \
            --type={dtype} \
            --overwrite \
            {all_bands_param}
    """,
        shell=True,
    )

    return cloud_mask


def create_cog(
    image_path: str,
    bounds: tuple[float, float, float, float],
    shape: tuple[int, int],
    crs: str,
    data_type: str,
    nodata: str | int | float,
    image_name: str,
    temp_folder: str,
    roi: str | None = None,
) -> str:
    """
    Function to create cloud optimized geotiff

    Args:
        image_path: path to the image. E.g `"/temp/image.tif"`
        bounds: bounding box of the region of interest. E.g `(110, 0, 111, 1)`
        shape: optimal shape of the image based on the bounding box and resolution. E.g `(1000, 1000)`
        crs: coordinate reference system. E.g `"EPSG:4326"`
        data_type: data type of the output image. E.g `"Float32"`
        nodata: no data value in the output. E.g `"NaN"`
        image_name: name of the image. E.g `"cool_image"`
        temp_folder: temporary folder to put the image. E.g `"/temp_x"`
        roi: path to region of interest for clipping. E.g `"/temp_x/roi.geojson"`
    Returns:
        str: path to the output image. E.g `"/temp_x/cool_image.tif"`
    """

    float_image = f"{temp_folder}/{image_name}.tif"
    crop_param = ""
    if roi is not None:
        crop_param = f"-cutline {roi}"
    check_call(
        f"""gdalwarp \
            -of COG \
            -co COMPRESS=ZSTD \
            -te {bounds[0]} {bounds[1]} {bounds[2]} {bounds[3]} \
            -ts {shape[1]} {shape[0]} \
            -t_srs {crs} \
            -ot {data_type} \
            -dstnodata {nodata} \
            {crop_param} \
            -overwrite \
            -wm {memory}G \
            -wo NUM_THREADS=ALL_CPUS \
            -multi \
            {image_path} \
            {float_image}
    """,
        shell=True,
    )
    return float_image


def median_composite(
    image_list: list[str],
    formula: str,
    image_name: str,
    temp_folder: str,
    band: int | None = None,
    nodata: int | str = 0,
    dtype: str = "UInt16",
) -> str:
    """
    Function to aggregate or median composite

    Args:
        image_list: paths to the image to aggregate. E.g `["/temp/image_1.tif", "/temp/image_2.tif"]`
        formula: formula to aggregate the image. E.g `"nanmedian(A,axis=0)"`
        image_name: name of the image. E.g `"cool_image"`
        temp_folder: temporary folder to put the image. E.g `"/temp_x"`
        band: no of band to use (if needed). E.g `1`

    Returns:
        str: path to the output image. E.g `"/temp_x/cool_image.tif"`
    """

    # Create median composite per band
    median_path = f"{temp_folder}/{image_name}.tif"
    band_param = ""
    if band is not None:
        band_param = f"--A_band={band}"

    check_call(
        f"""gdal_calc \
            -A {" ".join(image_list)} \
            {band_param} \
            --calc="{formula}" \
            --outfile={median_path} \
            --NoDataValue={nodata} \
            --co="COMPRESS=ZSTD" \
            --type={dtype} \
            --overwrite
    """,
        shell=True,
    )
    return median_path
