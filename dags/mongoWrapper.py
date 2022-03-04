
import json
import os
import pickle

import numpy as np
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.binary import Binary

from settings import MONGO_URI

class MongoClientWrapper:

    def __init__(self):
        self.client = MongoClient(MONGO_URI)




