import rpy2.robjects as robjects
import pandas as pd

from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

robjects.r("source('src/lib/DataGathering.r')")

make_acs_table_t_r = robjects.globalenv["make_acs_table_t"]
make_acs_table_bg_r = robjects.globalenv["make_acs_table_bg"]
