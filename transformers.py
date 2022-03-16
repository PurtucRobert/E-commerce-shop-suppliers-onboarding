from pyspark.ml.pipeline import Transformer
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from constants import car_body_type_dict
from constants import colors_dict
from constants import spark
from constants import schema
from constants import attribute_dict


class PreProcess(Transformer):
    """
    The PreProcess transformer will adjust the granularity of the data by doing
    a query on the Attribute Names and Values, grouped by ID and pivoting the Attribute Names.
    The resulted DataFrame consists of multiple columns, each column being one attribute,
    which is then joined to the the DataFrame from the supplier.
    The duplicates rows for each id are dropped thus reducing the number of rows
    from 9 to 1 for each id.
    As the extracted attributes names and values are already part of the data, the columns
    "Attribute Names" and "Attribute Values" are dropped
    The preprocessed DataFrame is then exported to an excel file named "PreProcess.xlsx"
    """

    def _transform(self, df):
        df.select("Attribute Names")
        df = (
            df.join(
                df.select("ID", "Attribute Names", "Attribute Values")
                .groupBy("ID")
                .pivot("Attribute Names")
                .agg(F.first("Attribute Values")),
                "ID",
                how="outer",
            )
            .drop_duplicates(["ID"])
            .drop("Attribute Names")
            .drop("Attribute Values")
        )
        df.toPandas().to_excel("PreProcess.xlsx")
        return df


class Normalisation(Transformer):
    """
    The normalisation transformer is required as some fields values are not
    matching the target(different spelling, language used, etc).
    The proposed Normalisation transformer will do the following:
    "BodyColorText" field is translated to English, then it capitalized
    and finally it's granularity is adjusted by removing the type of color
    (eg: metallic) as it's not matching the target value.
    "MakeText" field is reformated from uppercase to being only capitalized.
    "BodyTypeText" field is normalized by being translated from german to english and then
    matching the target values (eg: Cabriolet -> Convertible / Roadster).
    "Km" field is transformed from Integer to Float
    "ModelText" is reformated from uppercase to being only capitalied
    The translation for the "BodyColorText" and "BodyTypeText" was done manually ,
    offline based on a dict of words because an API key was needed for the providers when using
    the online solution(Google/Microsoft/Yandex)
    """

    def _transform(self, df):
        normalize_string = udf(lambda x: x.lower().title() if x else "null")
        normalize_color = udf(lambda x: colors_dict[x.split(" ")[0]].title())
        normalize_km = udf(lambda x: float(x))
        normalize_body_type = udf(lambda x: car_body_type_dict[x])
        df = (
            df.withColumn("makeText", normalize_string("makeText"))
            .withColumn("BodyColorText", normalize_color("BodyColorText"))
            .withColumn("Km", normalize_km("km"))
            .withColumn("BodyTypeText", normalize_body_type("BodyTypeText"))
            .withColumn("ModelText", normalize_string("ModelText"))
        )
        df.toPandas().to_excel("Normalisation.xlsx")
        return df


class Extraction(Transformer):
    """
    By making use of the Extraction transformer, a total of 4 aditional attributes were obtained:
    From the attribute "ConsumptionTotalText" were extracted
        - The measurement unit of the field in the attribute: "extracted-unit-ConsumptionTotalText"
        - The numerical part of the field in the attribute: "extracted-value-ConsumptionTotalText"
    From the attribute "Co2EmissionText" were extracted
        - The measurement unit of the field in the attribute: ""extracted-unit-Co2EmissionText"
        - The numerical part of the field in the attribute: "extracted-value-Co2EmissionText"
    """

    def _transform(self, df):
        df = (
            (
                df.withColumn(
                    "extracted-unit-ConsumptionTotalText",
                    F.split("ConsumptionTotalText", " ").getItem(1),
                )
                .withColumn(
                    "extracted-value-ConsumptionTotalText",
                    F.split("ConsumptionTotalText", " ").getItem(0),
                )
                .withColumn(
                    "extracted-unit-Co2EmissionText",
                    F.split("Co2EmissionText", " ").getItem(1),
                )
                .withColumn(
                    "extracted-value-Co2EmissionText",
                    F.split("Co2EmissionText", " ").getItem(0),
                )
            )
            .drop("ConsumptionTotalText")
            .drop("Co2EmissionText")
        )
        df.toPandas().to_excel("Extraction.xlsx")
        return df


class Integration(Transformer):
    """
    The integration of data was done using the following mappings
    (structure is given as "supplier attribute" -> "target attribute"):
    "BodyColorText": "color",
    "makeText": "make",
    "ModelText": "model",
    "TypeName": "model_variant",
    "City": "city",
    "Km": "mileage",
    "BodyTypeText": "carType",
    Mileage unit was hardcoded to kilometer as the mileage received from the
    supplier is always given in klilometers(check the "Km" field)
    fuel_consumption_unit was mapped to the extracted unit from ConsumptionTotalText attribute
    if the unit is l/100km, it will be mapped to the target "l_km_consumption" value
    otherwise it will be null as I did not knew the other posibilites.
    carType attribute was mapped to the normalised attribute BodyTypeText
    type was integrated based on the value of carType. Entries with the attribute carType 'null'
    are having 'null' value, otherwise they have 'car' value.
    """

    def _transform(self, df):
        df_integrated = spark.createDataFrame([], schema)
        df_integrated = df_integrated.toPandas()
        df = df.toPandas()
        for attribute in attribute_dict:
            df_integrated[attribute_dict[attribute]] = df[attribute]
        df_integrated["mileage_unit"] = "kilometer"
        df_integrated["fuel_consumption_unit"] = "l_km_consumption"
        df_integrated["fuel_consumption_unit"] = df[
            "extracted-unit-ConsumptionTotalText"
        ].apply(lambda x: "l_km_consumption" if x == "l/100km" else "null")
        df_integrated = df_integrated.fillna("null")
        df_integrated["type"] = df_integrated["carType"].apply(
            lambda x: "null" if x == "Null" else "car"
        )
        df_integrated.to_excel("Integration.xlsx")
        df_integrated = spark.createDataFrame(df_integrated, schema).show()
