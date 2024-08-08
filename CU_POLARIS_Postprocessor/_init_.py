from .clean import clean_analysis
from .config import PostProcessingConfig
from .parallel import parallel_process_folders
from .postprocessing import *
from .prerun import *

default_config = PostProcessingConfig(param1='default_value')