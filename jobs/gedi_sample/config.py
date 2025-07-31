import json
from os import getenv

from dotenv import load_dotenv

load_dotenv()

START_DATE = getenv("GEDI_START_DATE")
END_DATE = getenv("GEDI_END_DATE")
ROI = f"input/{getenv('GEDI_INPUT_ROI')}"
OUTPUT_PREFIX = getenv("GEDI_OUTPUT_PREFIX")
SOURCE = getenv("GEDI_SOURCE", "earthaccess")

GEDI_IDS_TEMP = json.loads(getenv("GEDI_IDS", "['L2A']"))
dict_gedi = dict(L2A="GEDI02_A", L2B="GEDI02_B", L4A="GEDI04_A")

GEDI_IDS = list(map(lambda x: dict_gedi[x], GEDI_IDS_TEMP))
GEDI_RH = int(getenv("GEDI_RH", "98"))

PARAMETERS = dict(
    GEDI02_A=dict(
        band=f"rh{GEDI_RH}",
        column="CHM",
        multiplier=1,
        quality_flag=["quality_flag"],
    ),
    GEDI02_B=dict(
        band="cover",
        column="treecover",
        multiplier=100,
        quality_flag=["l2b_quality_flag"],
    ),
    GEDI04_A=dict(
        band="agbd",
        column="AGB",
        multiplier=1,
        quality_flag=["l2_quality_flag", "l4_quality_flag"],
    ),
)
