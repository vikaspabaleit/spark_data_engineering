This is the spark_data_engineering project where we are covering all technical concepts, including GCP.

spark_data_engineering/
│── README.md
│── requirements.txt
│── setup.py
│── .gitignore
│
├── configs/
│   ├── application.conf         # Configs for dev/prod
│   └── gcp_keys.json            # (keep in .gitignore!)
│
├── data/
│   ├── input/                   # Local testing data
│   └── output/                  # Local output
│
├── src/
│   ├── main/
│   │   ├── spark/
│   │   │   └── com/python/      # Core ETL code
│   │   │       ├── extractor/   # Extract logic
│   │   │       ├── transformer/ # Transform logic
│   │   │       ├── loader/      # Load into sinks (GCS/BigQuery)
│   │   │       └── utils/       # Common helpers
│   │   └── jobs/                # Job scripts for specific pipelines
│   └── test/                    # Unit tests
│
├── tests/
│   ├── conftest.py
│   ├── test_transformations.py
│   ├── test_loader.py
│   └── test_utils.py
│
└── notebooks/
    └── exploration.ipynb        # For data exploration / POC
