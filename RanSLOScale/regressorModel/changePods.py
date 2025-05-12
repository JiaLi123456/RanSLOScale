import time
from monitor import podChange
from config.Config import Config
import warnings
warnings.filterwarnings("ignore")


if __name__ == '__main__':
    config = Config()
    podChange.collect(config)