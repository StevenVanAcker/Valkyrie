#!/usr/bin/env python3

from valkyrie import ValkyrieMaster
import logging

if __name__ == "__main__":
    recipe = {}
    master = ValkyrieMaster(recipe)
    master.logger.setLevel(logging.DEBUG)
    master.run()





