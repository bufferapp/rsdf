from pandas import DataFrame
from .redshift import to_redshift
from .s3 import to_s3

setattr(DataFrame, to_redshift.__name__, to_redshift)
setattr(DataFrame, to_s3.__name__, to_s3)
