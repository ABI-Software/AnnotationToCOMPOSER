import os

class Config(object):
    ANNOTATION_SECRET = os.environ.get("ANNOTATION_SECRET")
    FLATMAP_URL =  os.environ.get("FLATMAP_URL")
