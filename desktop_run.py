#!/usr/bin/env python3

import os
from utils import get_config
from run import main

if __name__ == "__main__":
    config_vars = get_config()
    os.environ.update(config_vars)
    main()