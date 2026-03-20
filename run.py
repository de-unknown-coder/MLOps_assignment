import numpy as np
import pandas as pd 
import yaml
import time
import logging
import json
import sys

CONFIG_PATH = 'config.yaml'
DATA_PATH = 'data.csv'
METRICS_PATH = 'metrics.json'
LOG_PATH = 'run.log'

def setup_logging():
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    return config

def validate_config(config):
    required_keys = ['seed', 'window', 'version']
    if config is None:
        raise ValueError("Config file is empty")
    if not isinstance(config, dict):
        raise ValueError("Invalid config format")
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")
    if not isinstance(config['seed'], int):
        raise ValueError("Config 'seed' must be an integer")
    if not isinstance(config['window'], int):
        raise ValueError("Config 'window' must be an integer")
    if not isinstance(config['version'], str):
        raise ValueError("Config 'version' must be a string")

def load_data():
    df = pd.read_csv(DATA_PATH, header=None)
    df = df.iloc[:, 0].str.split(",", expand=True)
    df.columns = df.iloc[0]
    df = df[1:]
    df.columns = df.columns.str.replace('"', '').str.strip().str.lower()
    df = df.apply(lambda col: col.str.replace('"', '').str.strip())
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df

def validate_dataset(df):
    if df is None or df.empty:
        raise ValueError("Dataset is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    if df["close"].isnull().all():
        raise ValueError("Column 'close' has no valid values")
    
def compute_rolling_mean(df, window):
    df["rolling_mean"] = df["close"].rolling(window).mean()
    return df

def generate_signal(df):
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    return df

def compute_metrics(df, start_time, config):
    rows_processed = len(df)
    signal_rate = df["signal"].mean()
    latency_ms = int((time.time() - start_time) * 1000)

    return {
        "version": config["version"],
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": float(round(signal_rate, 4)),
        "latency_ms": latency_ms,
        "seed": config["seed"],
        "status": "success"
    }

def main():
    start_time = time.time()
    setup_logging()
    logging.info("Job started")

    try:
        config = load_config()
        logging.info(f"Config loaded: {config}")

        validate_config(config)
        logging.info("Config validation passed")

        df = load_data()
        logging.info(f"Data loaded with shape: {df.shape}")
        
        validate_dataset(df)
        logging.info("Dataset validated")

        df = compute_rolling_mean(df, config["window"])
        logging.info("Rolling mean computed")

        df = generate_signal(df)
        logging.info("Signal generated")

        metrics = compute_metrics(df, start_time, config)

        with open(METRICS_PATH, "w") as f:
            json.dump(metrics, f, indent=2)

        print(json.dumps(metrics, indent=2))
        logging.info(f"Metrics: {metrics}")
        logging.info("Job finished successfully")

    except Exception as e:
        logging.exception("Job failed")

        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        with open(METRICS_PATH, "w") as f:
            json.dump(error_metrics, f, indent=2)

        print(json.dumps(error_metrics, indent=2))

        sys.exit(1)

if __name__ == "__main__":
    main()
    
