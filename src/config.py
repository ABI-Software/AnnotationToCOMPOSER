import os

class Config(object):
    ANNOTATION_SECRET = os.environ.get("ANNOTATION_SECRET")
