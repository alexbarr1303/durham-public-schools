# functions to load data
from lib.dataloader_alex import get_parcels, get_du_est, add_columns_from_csv

# get functions to get the census data by tract and bg
from lib.census import make_acs_table_t_r, make_acs_table_bg_r


import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter


acs_table_t = make_acs_table_t_r(2020)
acs_table_bg = make_acs_table_bg_r(2020)


# Convert the R DataFrame to a pandas DataFrame
with localconverter(robjects.default_converter + pandas2ri.converter):
    table_t = robjects.conversion.rpy2py(make_acs_table_t_r)
    table_bg = robjects.conversion.rpy2py(make_acs_table_bg_r)


# aggregation
# - potential regression for the rent values
