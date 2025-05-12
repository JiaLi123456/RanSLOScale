import time
rom PBScaler import PBScaler
from monitor import MetricCollect
from config.Config import Config
import warnings
import sys
warnings.filterwarnings("ignore")


if __name__ == '__main__':
    config = Config()
    if len(sys.argv) != 2:
        print("Usage: python3 main.py <data_path>")
        sys.exit(1)

    data_path = sys.argv[1]
    MetricCollect.collect(config, data_path)
