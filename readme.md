# Remote Sensing Analysis With Python Scripts Collection #

Hi Geospatial Enthusiast!

I have been sharing many of my scripts about remote sensing analysis such as land cover classification, or deep learning. Now I want to combined it into one repository. Thus no need one repository for each analysis.

For now only three scripts area available, next time will be more. I will probably updated this every month or when I make a new scripts.

### Scripts catalog ###
1. [Cloudless and Topograhic Corrected Landsat Composite](scripts/landsat_cloudless_topographic/landsat_cloudless_topographic.ipynb)
2. [Land Cover Classification](scripts/landcover_classification/landcover_classification.ipynb)
3. [Deep Learning Land Cover Classification with U-Net](scripts/deeplearning_landcover_classification_unet/modelling.ipynb)

### General Instructions Without Docker ###
To make sure you can run scripts as expected, do:
1. Install Python 3.9 - 3.12.
2. Create virtual environment at the base of this repository with `python -m venv .venv`.
3. Activate the virtual environment with `.venv\Scripts\activate`.
4. Install all the necessary packages from [`requirements.txt`](requirements.txt) with `pip install -r requirements.txt`.
5. Open and run the script from the catalog.

### General Instructions With Docker JupyterLab ###
To make sure you can run scripts as expected, do:
1. Run `docker compose up --build`
2. Open `http://127.0.0.1:8888/lab` in the browser
3. Open and run the script from the catalog

Created by Ramadhan

[Email](ramiqcom@gmail.com)
[LinkedIn](https://linkedin.com/in/ramiqcom)
[GitHub](https://github.com/ramiqcom)
[Youtube](https://youtube.com/@ramiqcom)
