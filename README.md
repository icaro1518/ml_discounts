# ml-discounts
Generación de sugerencias de descuentos para productos de MercadoLibre

## Descripción del Proyecto
Este proyecto tiene como objetivo generar sugerencias de descuentos para productos de MercadoLibre. El flujo de trabajo consta de varios notebooks que abarcan diferentes etapas del proceso, incluyendo la extracción de datos, el preprocesamiento y la ingeniería de características, el análisis exploratorio de datos (EDA) y la construcción de modelos.

## Notebooks

### 1. Data Extraction
En este notebook se realiza la extracción de datos de MercadoLibre, obteniendo información relevante sobre los productos y sus características. Esto se realiza a través de la API de Mercadolibre con un access token, buscando extraer datos de items, sellers y ratings (estos últimos no se lograron extraer finalmente debido a los tiempos de ejecución para obtener estos datos).

### 2. Preprocessing and Feature Engineering
En este notebook se realiza el preprocesamiento de los datos extraídos, incluyendo la limpieza, transformación y selección de características. También se lleva a cabo la ingeniería de características para crear variables adicionales que puedan ser útiles para los modelos.

### 3. EDA (Exploratory Data Analysis)
En este notebook se realiza un análisis exploratorio de los datos preprocesados, con el objetivo de comprender mejor las relaciones entre las variables y obtener insights relevantes para la construcción de los modelos.

### 4. Models
En este notebook se construyen los modelos finales. Se desarrolla un modelo de clasificación para determinar si un producto debería tener un descuento o no, y otro modelo para definir la tasa de descuento adecuada. Para el caso del modelo de clasificación se utilizaron dos modelos: XGBoost Classifier (modelo final elegido) y support vector machines. Para el caso del modelo de regresión se probaron también 2 modelos: XGBoost Regressor (modelo final elegido) y lasso regression.

### 5. Evaluation

**XGBoost Classifier (test results)**

|   | precision | recall | f1-score | support |
|---|-----------|--------|----------|---------|
| 0 | 0.82      | 0.78   | 0.80     | 3686    |
| 1 | 0.80      | 0.84   | 0.82     | 3796    |

AUC = 0.8108

**XGBoost Regressor (test results)**

| Metric | Value  |
|--------|--------|
| RMSE   | 0.9037 |
| MAPE   | 0.5869 |
| R2     | 0.5433 |

## Requisitos
- python = ">=3.9,<3.11"
- pandas = ">=2.0.0"
- requests = "^2.31.0"
- plotnine = "^0.13.0"
- ipykernel = "^6.29.2"
- pyarrow = "^15.0.0"
- pandas-profiling = "^3.6.6"
- ipywidgets = "^8.1.2"
- kaleido = "0.2.1"
- scikit-learn = "^1.4.1.post1"
- xgboost = "^2.0.3"
- matplotlib = "^3.8.3"
- setuptools = "^69.1.1"
- imbalanced-learn = "^0.12.0"

## Instrucciones de Uso
1. Clona este repositorio en tu máquina local.
2. Instala las bibliotecas de Python mencionadas en los requisitos (se encuentran disponibles en pyproject.toml).
3. Abrir los notebooks en Jupyter Notebook, cada notebook se encuentra enumerado para poder guiarse en el proceso. **NOTA:** Hay algunos pasos en los notebooks que se pueden saltar al contar ya con los datos recopilados, sin embargo, se pueden ejecutar totalmente para que se realice también el proceso de extracción de datos (para esto es importante contar con un token de acceso a una aplicación para acceder a la API de ML).
4. Ejecuta cada celda de código en orden, siguiendo las instrucciones y comentarios proporcionados en los notebooks.

## Disponibilidad de datos API Mercadolibre
Para poder utilizar las funcionalidades de acceso a la API de ML es importante llenar los contenidos de los archivos ubicados en la carpeta *scripts/data/secrets*.

## Disponibilidad de datos compilados (24/02/2024)
Se agregan los datos recopilados para el ejercicio en la carpeta data para facilitar la prueba.

