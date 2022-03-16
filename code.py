from pyspark.ml import Pipeline
from transformers import PreProcess
from transformers import Normalisation
from transformers import Extraction
from transformers import Integration
import constants as Constants


df_supplier = Constants.spark.read.json(Constants.JSON_PATH)


preprocess = PreProcess()
normalisation = Normalisation()
extraction = Extraction()
integration = Integration()
pipeline = Pipeline(stages=[preprocess, normalisation, extraction, integration])
Featpip = pipeline.fit(df_supplier)
df = Featpip.transform(df_supplier)
