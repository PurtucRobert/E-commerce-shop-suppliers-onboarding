from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

spark = (
    SparkSession.builder.master("local").appName("DataEngineeringTask").getOrCreate()
)

JSON_PATH = "supplier_car.json"
CSV_PATH = "Target Data.xlsx"
# Define custom schema
schema = StructType(
    [
        StructField("carType", StringType(), True),
        StructField("color", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("drive", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("make", StringType(), True),
        StructField("manufacture_year", StringType(), True),
        StructField("mileage", StringType(), True),
        StructField("mileage_unit", StringType(), True),
        StructField("model", StringType(), True),
        StructField("model_variant", StringType(), True),
        StructField("price_on_request", StringType(), True),
        StructField("type", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("manufacture_month", StringType(), True),
        StructField("fuel_consumption_unit", StringType(), True),
    ]
)
colors_dict = {
    "beige": "Beige",
    "schwarz": "Black",
    "blau": "Blue",
    "braun": "Brown",
    "gold": "Gold",
    "grau": "Gray",
    "grün": "Green",
    "orange": "Orange",
    "rot": "Red",
    "silber": "Silver",
    "weiss": "White",
    "gelb": "Yellow",
    "bordeaux": "Other",
    "anthrazit": "Other",
    "violett": "Other",
}

car_body_type_dict = {
    "Limousine": "Saloon",
    None: "Null",
    "SUV / Geländewagen": "SUV",
    "Sattelschlepper": "Custom",
    "Cabriolet": "Convertible / Roadster",
    "Kompaktvan / Minivan": "Custom",
    "Kleinwagen": "Custom",
    "Wohnkabine": "Custom",
    "Pick-up": "Custom",
    "Kombi": "Station Wagon",
    "Coupé": "Coupe",
}

attribute_dict = {
    "BodyColorText": "color",
    "makeText": "make",
    "ModelText": "model",
    "TypeName": "model_variant",
    "City": "city",
    "Km": "mileage",
    "BodyTypeText": "carType",
}
