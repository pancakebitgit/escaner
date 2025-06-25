# Escáner de Actividad de Dark Pool

Este proyecto analiza archivos CSV de datos de opciones para detectar posible actividad de transacciones en "dark pools". La lógica se basa en la comparación del volumen y el open interest (OI) de contratos de opciones entre dos días consecutivos.

## Lógica Principal

La fórmula utilizada para inferir actividad de dark pool para un contrato específico es:

`ActividadDarkPool = (Volumen_Día1 + OpenInterest_Día1) - OpenInterest_Día2`

Donde:
- `Volumen_Día1`: Es el volumen de la última transacción registrada para el contrato en el Día 1.
- `OpenInterest_Día1`: Es el open interest registrado en la última transacción para el contrato en el Día 1.
- `OpenInterest_Día2`: Es el open interest registrado en la primera transacción para el mismo contrato en el Día 2.

Si `ActividadDarkPool > 0`, se considera una posible transacción no registrada públicamente (dark pool) el Día 1.

## Estructura del Proyecto

```
darkpool_scanner/
├── data/                     # Directorio para archivos CSV de entrada (ej. YYYY-MM-DD.csv)
│   ├── 2025-06-12.csv
│   └── 2025-06-13.csv
├── src/                      # Código fuente del scanner
│   ├── __init__.py
│   └── scanner.py            # Lógica principal de procesamiento y detección
├── tests/                    # Pruebas unitarias
│   ├── __init__.py
│   └── test_scanner.py       # Pruebas para scanner.py
├── main.py                   # Script principal para ejecutar el scanner
├── requirements.txt          # Dependencias del proyecto
└── README.md                 # Este archivo
```

## Requisitos

- Python 3.8+
- pandas

## Instalación

1.  Clonar el repositorio (si aplica).
2.  Navegar al directorio raíz del proyecto: `cd darkpool_scanner`
3.  Se recomienda crear un entorno virtual:
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```
4.  Instalar las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso

El script `main.py` es el punto de entrada para ejecutar el escáner.

### Opciones de Línea de Comandos

-   **Procesar un par de archivos específico:**
    ```bash
    python main.py --file_d1 data/YYYY-MM-DD_dia1.csv --file_d2 data/YYYY-MM-DD_dia2.csv
    ```
    Ejemplo:
    ```bash
    python main.py --file_d1 data/2025-06-12.csv --file_d2 data/2025-06-13.csv
    ```

-   **Procesar un directorio de archivos CSV:**
    El script buscará archivos `YYYY-MM-DD.csv` en el directorio especificado y procesará pares de días consecutivos automáticamente.
    ```bash
    python main.py --dir data/
    ```

-   **Guardar los resultados en un archivo CSV:**
    Añada el argumento `--output` a cualquiera de los comandos anteriores.
    ```bash
    python main.py --dir data/ --output resultados_darkpool.csv
    python main.py --file_d1 data/2025-06-12.csv --file_d2 data/2025-06-13.csv --output resultado_par_especifico.csv
    ```

### Formato de los Archivos CSV de Entrada

Los archivos CSV deben contener como mínimo las siguientes columnas (los nombres exactos pueden variar ligeramente, pero la primera columna debe ser el identificador del contrato y las otras deben estar presentes):
-   `Symbol,Symbol,Price~` (o la primera columna que actúa como Identificador del Contrato, ej: `AAPL|20250620|235.00P`)
-   `Volume`
-   `"Open Int"` (o `Open Int`)
-   `Time` (usada para determinar la primera/última transacción si hay múltiples por contrato, aunque la implementación actual toma la primera/última aparición en el archivo)

## Desarrollo y Pruebas

-   **Código Fuente:** La lógica de procesamiento se encuentra en `src/scanner.py`.
-   **Pruebas Unitarias:** Las pruebas para `scanner.py` están en `tests/test_scanner.py`. Para ejecutar las pruebas (desde el directorio raíz `darkpool_scanner`):
    ```bash
    python -m unittest discover tests
    ```
    o directamente:
    ```bash
    python tests/test_scanner.py
    ```

## Comentarios Adicionales en el Código

El código en `src/scanner.py` incluye docstrings para cada función explicando su propósito, argumentos y lo que retorna. Se han añadido comentarios en puntos clave para clarificar la lógica.

-   `read_csv_to_dataframe()`: Maneja la lectura y limpieza inicial de nombres de columna.
-   `get_last_transactions_day1()`: Extrae los datos relevantes (Volumen, Open Interest) de la última transacción del Día 1 para cada contrato.
-   `get_first_transaction_open_interest_day2()`: Extrae el Open Interest de la primera transacción del Día 2 para cada contrato.
-   `detect_dark_pool_activity()`: Combina los datos de los dos días y aplica la fórmula de detección.

El script `main.py` está comentado para explicar el flujo de procesamiento de argumentos y la orquestación de llamadas a las funciones del scanner.
Los archivos de prueba en `tests/test_scanner.py` también contienen comentarios que explican el propósito de cada test.Tool output for `create_file_with_block`:
