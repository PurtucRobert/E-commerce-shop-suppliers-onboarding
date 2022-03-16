# E-commerce-shop-suppliers-onboarding

The purpose of this repo is to provide an example of how an e-commerce shop should onboard new suppliers efficiently.

Steps done in order to achieve the target data:
 ## Data Preprocessing ##
The data preprocessing stage will adjust the granularity of the data by doing
a query on the Attribute Names and Values, grouped by ID and pivoting the Attribute Names.
The resulted DataFrame consists of multiple columns, each column being one attribute,
which is then joined to the the DataFrame from the supplier.
The duplicates rows for each id are dropped thus reducing the number of rows
from 9 to 1 for each id.
As the extracted attributes names and values are already part of the data, the columns
"Attribute Names" and "Attribute Values" are dropped.

The preprocessed DataFrame is then exported to an excel file named "PreProcess.xlsx"

## Data Normalisation ###
At this step, data was normalised to match the target attributes. 
- "BodyColorText" field is translated to English, then it capitalized
and finally it's granularity is adjusted by removing the type of color
(eg: metallic) as it's not matching the target value.
- "MakeText" field is reformated from uppercase to being only capitalized.
- "BodyTypeText" field is normalized by being translated from german to english and then
matching the target values (eg: Cabriolet -> Convertible / Roadster).
- "Km" field is transformed from Integer to Float
- "ModelText" is reformated from uppercase to being only capitalised

The translation for the "BodyColorText" and "BodyTypeText" was done manually ,
offline based on a dict of words because an API key was needed for the providers when using
the online solution(Google/Microsoft/Yandex).

The normalised data is exported to: "Normalisation.xlsx"

## Data Extraction ###
In the extraction stage a total of 4 aditional attributes were obtained from the already present dataset.
From the attribute "ConsumptionTotalText" were extracted
- The measurement unit of the field in the attribute: "extracted-unit-ConsumptionTotalText"
- The numerical part of the field in the attribute: "extracted-value-ConsumptionTotalText"

From the attribute "Co2EmissionText" were extracted
- The measurement unit of the field in the attribute: ""extracted-unit-Co2EmissionText"
 - The numerical part of the field in the attribute: "extracted-value-Co2EmissionText"

The results of this step is saved in: "Extraction.xlsx"

## Data Integration ###
The integration of data was done using the following mappings
(structure is given as "supplier attribute" -> "target attribute"):


| Original Attribute  | Target Attribute |
| ------------- | ------------- |
| BodyColorText  | color  |
| makeText  | make  |
| ModelText  | model  |
| TypeName  | model_variant  |
| City  | city  |
| Km  | mileage  |
| BodyTypeText  | carType  |

### Integration of the data 

- Mileage unit was hardcoded to kilometer as the mileage received from the supplier is always given in klilometers(check the "Km" field)
- fuel_consumption_unit was mapped to the extracted unit from ConsumptionTotalText attribute if the unit is l/100km, it will be mapped to the target "l_km_consumption" value otherwise it will be null as I did not knew the other posibilites.
- carType attribute was mapped to the normalised attribute BodyTypeText
- type was integrated based on the value of carType. Entries with the attribute carType 'null' are having 'null' value, otherwise they have 'car' value.

The results of this step is saved in: "Integration.xlsx"

## Instalation
```bash
pip install -r requirements.txt
```

## Usage

```bash
python3 code.py
```

After the PySpark pipeline is ran, 4 files are obtained. 
