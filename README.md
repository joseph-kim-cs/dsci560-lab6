Create a python venv: python -m venv .venv

Activate the virtual environment: .venv/Scripts/activate, or source .venv/bin/activate

Install all required packages: pip install -r requirements.txt

Create a docker container: docker compose up -d
* make sure that docker is installed locally and docker is running

Make a copy of the env, and rename to .env

To run the pipeline: python scripts/pipeline.py



Additional info: data from the dsci560 lab drive folder is in `data/`, but unzipped and used as data directory. 


Scripts: 

extract_pdf.py: extracts data from the pdf data through pypdf

pipeline.py: data pipeline script for mysql database, including functions and schema - can move data from extract_pdf.py into the mysql database