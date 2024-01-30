#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import sys

from airbyte_cdk.entrypoint import launch
from source_sosha import SourceSosha

if __name__ == "__main__":
    source = SourceSosha()
    launch(source, sys.argv[1:])