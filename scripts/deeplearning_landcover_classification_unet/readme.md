# Land Cover Classification Using Deep Learning U-Net #

This repository is sets of scripts that I developed that could do land cover classfication using [U-Net](https://arxiv.org/abs/1505.04597]) model, Landsat imagery as predictor, and Indonesia Ministry of Life Environment and Forestry data as land cover label.

## Instruction ##
Before running the scripts, you need to:
1. Instal Python at least version 3.9.
2. Create virtual environment.
3. Install the required package in the [`requirements.txt`](../../requirements.txt) using `pip install -r requirements.txt`.
4. Create folder named `lcs` and `images` inside `data` folder. This is as folder to save generated sample data

## Guide to the Scripts ##
This modelling consisted of two scripts:
1. [`preprocess.ipynb`](preprocess.ipynb).

	This script purpose is to generate the sample data on which to trained the model. It will load Landsat imagery and raster land cover from cloud storage then turn it into multiple grid/patch of smaller image/map to used to train the model. Then saved the the patch in the your local drive which can be loaded in [`modelling.ipynb`](modelling.ipynb) script.

2. [`modelling.ipynb`](modelling.ipynb)

	This script purpose is to generate and train the land cover model. It will load the patches from the [`preprocess.ipynb`](preprocess.ipynb) script, split it into train and test sets, used to fit the model, assess the model, and visualize the difference between the actual test result and its prediction, and saved the model (for later used maybe).
