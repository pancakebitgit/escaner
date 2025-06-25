import pandas as pd

def read_csv_to_dataframe(file_path: str) -> pd.DataFrame | None:
    """
    Lee un archivo CSV y lo convierte en un DataFrame de pandas.

    Args:
        file_path: Ruta al archivo CSV.

    Returns:
        Un DataFrame de pandas o None si ocurre un error.
    """
    try:
        df = pd.read_csv(file_path)
        # La primera columna es el identificador del contrato, ej: 'AAPL|20250620|235.00P'
        # Renombramos para claridad si es necesario, o la usamos directamente.
        # La primera columna contiene el identificador del contrato.
        # Es importante manejar el nombre de esta columna correctamente.
        # El nombre real en el CSV es 'Symbol,Symbol,Price~'
        # Lo renombraremos a 'ContractIdentifier' para facilitar su uso.
        if 'Symbol,Symbol,Price~' in df.columns:
            df.rename(columns={'Symbol,Symbol,Price~': 'ContractIdentifier'}, inplace=True)
        elif df.columns[0] != 'ContractIdentifier': # Si ya fue renombrado o tiene otro nombre
            # Esta es una heurística, podría necesitar ajuste si la estructura del CSV cambia.
            print(f"Advertencia: La columna esperada 'Symbol,Symbol,Price~' no se encontró. Usando la primera columna '{df.columns[0]}' como 'ContractIdentifier'.")
            df.rename(columns={df.columns[0]: 'ContractIdentifier'}, inplace=True)

        # Limpiar nombres de todas las columnas para remover comillas y espacios extra.
        df.columns = df.columns.str.replace('"', '').str.strip()

        return df
    except FileNotFoundError:
        print(f"Error: El archivo {file_path} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Error al leer el archivo CSV {file_path}: {e}")
        return None

def get_last_transactions_day1(df_day1: pd.DataFrame) -> pd.DataFrame | None:
    """
    Encuentra la última transacción para cada contrato en el DataFrame del Día 1.

    Args:
        df_day1: DataFrame con los datos de las opciones del Día 1.

    Returns:
        Un DataFrame con ['ContractIdentifier', 'Volume', 'Open Int'] para la última transacción
        de cada contrato, o None si el DataFrame de entrada es None.
    """
    if df_day1 is None:
        return None
    # Asegurarse de que la columna 'Time' es interpretable para ordenamiento si es necesario.
    # Por ahora, asumimos que el CSV está ordenado por tiempo o que la última aparición es la última transacción.
    # Si no, necesitaríamos convertir 'Time' a datetime y ordenar.
    # Ejemplo: df_day1['Time'] = pd.to_datetime(df_day1['Time'].str.replace(' ET', ''), format='%H:%M:%S')
    # df_day1 = df_day1.sort_values(by=['ContractIdentifier', 'Time'])

    # Agrupar por el identificador del contrato y tomar la última entrada de cada grupo.
    # Asumimos que 'ContractIdentifier' ya es el nombre de la columna identificadora.
    contract_col_name = 'ContractIdentifier'
    if contract_col_name not in df_day1.columns:
        print(f"Error: La columna '{contract_col_name}' no se encuentra en df_day1. Columnas disponibles: {df_day1.columns}")
        return None

    last_transactions = df_day1.groupby(contract_col_name, sort=False).tail(1) # sort=False para mantener el orden original si es relevante y ya está ordenado por tiempo

    # Seleccionar solo las columnas relevantes: Identificador, Volumen y Open Interest.
    # Los nombres de columna ya deberían estar limpios por read_csv_to_dataframe.
    try:
        # Los nombres esperados después de la limpieza son 'Volume' y 'Open Int'
        relevant_cols = [contract_col_name, 'Volume', 'Open Int']
        for col in relevant_cols[1:]: # Chequear 'Volume' y 'Open Int'
            if col not in last_transactions.columns:
                print(f"Error: La columna '{col}' no se encontró después de agrupar en df_day1. Columnas disponibles: {last_transactions.columns}")
                return None

        last_transactions_processed = last_transactions[relevant_cols].copy()
        last_transactions_processed.rename(columns={'Open Int': 'OpenInt_D1', 'Volume': 'Volume_D1'}, inplace=True)
    except KeyError as e:
        # Este error no debería ocurrir si la limpieza y el chequeo anterior funcionaron.
        print(f"Error inesperado al seleccionar columnas en df_day1: {e}. Columnas disponibles: {last_transactions.columns}")
        return None

    return last_transactions_processed.set_index(contract_col_name)


def get_first_transaction_open_interest_day2(df_day2: pd.DataFrame) -> pd.DataFrame | None:
    """
    Encuentra el Open Interest de la primera transacción para cada contrato en el DataFrame del Día 2.

    Args:
        df_day2: DataFrame con los datos de las opciones del Día 2.

    Returns:
        Un DataFrame con ['ContractIdentifier', 'OpenInt_D2'] para la primera transacción
        de cada contrato, o None si el DataFrame de entrada es None.
    """
    if df_day2 is None:
        return None
    # Similar a day1, podríamos necesitar ordenar por tiempo si no está garantizado.
    # Asumimos que 'ContractIdentifier' ya es el nombre de la columna identificadora.
    contract_col_name = 'ContractIdentifier'
    if contract_col_name not in df_day2.columns:
        print(f"Error: La columna '{contract_col_name}' no se encuentra en df_day2. Columnas disponibles: {df_day2.columns}")
        return None

    first_transactions = df_day2.groupby(contract_col_name, sort=False).head(1)

    try:
        # Los nombres de columna ya deberían estar limpios por read_csv_to_dataframe.
        # La columna de interés es 'Open Int'.
        if 'Open Int' not in first_transactions.columns:
            print(f"Error: La columna 'Open Int' no se encontró después de agrupar en df_day2. Columnas disponibles: {first_transactions.columns}")
            return None

        first_transactions_processed = first_transactions[[contract_col_name, 'Open Int']].copy()
        first_transactions_processed.rename(columns={'Open Int': 'OpenInt_D2'}, inplace=True)
    except KeyError as e:
        # Este error no debería ocurrir si el chequeo anterior funcionó.
        print(f"Error inesperado al seleccionar columnas en df_day2: {e}. Columnas disponibles: {first_transactions.columns}")
        return None

    return first_transactions_processed.set_index(contract_col_name)

def detect_dark_pool_activity(df_day1_processed: pd.DataFrame | None, df_day2_processed: pd.DataFrame | None) -> pd.DataFrame:
    """
    Detecta la actividad de dark pool comparando los datos procesados del Día 1 y Día 2.

    Args:
        df_day1_processed: DataFrame con Volume_D1 y OpenInt_D1 (indexado por ContractIdentifier).
                           Resultado de get_last_transactions_day1. Puede ser None.
        df_day2_processed: DataFrame con OpenInt_D2 (indexado por ContractIdentifier).
                           Resultado de get_first_transaction_open_interest_day2. Puede ser None.

    Returns:
        Un DataFrame con los contratos que muestran actividad de dark pool y la cantidad.
        Retorna un DataFrame vacío si no se detecta actividad, si hay errores en los datos de entrada,
        o si alguno los DataFrames de entrada es None o está vacío.
    """
    if df_day1_processed is None or df_day1_processed.empty:
        return pd.DataFrame()
    if df_day2_processed is None or df_day2_processed.empty:
        return pd.DataFrame()

    merged_df = df_day1_processed.join(df_day2_processed, how='inner')

    if merged_df.empty:
        return pd.DataFrame()

    cols_to_numeric = ['Volume_D1', 'OpenInt_D1', 'OpenInt_D2']
    all_cols_present = True
    for col in cols_to_numeric:
        if col not in merged_df.columns:
            all_cols_present = False
            break
    if not all_cols_present:
        return pd.DataFrame()

    for col in cols_to_numeric:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    merged_df.dropna(subset=cols_to_numeric, inplace=True)

    if merged_df.empty:
        return pd.DataFrame()

    # Fórmula corregida: OpenInt_D2 - (Volume_D1 + OpenInt_D1)
    merged_df['DarkPoolActivity'] = merged_df['OpenInt_D2'] - (merged_df['Volume_D1'] + merged_df['OpenInt_D1'])

    dark_pool_trades = merged_df[merged_df['DarkPoolActivity'] > 0].copy()

    # Devolver solo las columnas relevantes, incluyendo el ContractIdentifier que es el índice.
    return dark_pool_trades[['Volume_D1', 'OpenInt_D1', 'OpenInt_D2', 'DarkPoolActivity']]


if __name__ == '__main__':
    # Este bloque __main__ ahora prueba la cadena completa con datos de ejemplo
    # 1. Crea DataFrames crudos (como si vinieran de read_csv_to_dataframe)
    # 2. Procesa estos DataFrames con get_last_transactions_day1 y get_first_transaction_open_interest_day2
    # 3. Llama a detect_dark_pool_activity con los DataFrames procesados.

    # Datos de ejemplo para Día 1 (simulando entrada directa a las funciones de procesamiento)
    df_raw_d1 = pd.DataFrame({
        'ContractIdentifier': ['AAPL|20250620|235.00P', 'AAPL|20250620|235.00P', 'MSFT|20250620|400.00C', 'GOOG|20250620|150.00C'],
        'Volume': [100, 150, 50, 200],
        'Open Int': [1000, 1050, 500, 300], # Esta columna se usará para OpenInt_D1
        'Time': ['10:00:00 ET', '10:05:00 ET', '10:02:00 ET', '10:10:00 ET']
        # Columnas adicionales que read_csv_to_dataframe manejaría pero no son críticas aquí
    })

    # Datos de ejemplo para Día 2 (simulando entrada directa a las funciones de procesamiento)
    df_raw_d2 = pd.DataFrame({
        'ContractIdentifier': ['AAPL|20250620|235.00P', 'AAPL|20250620|235.00P', 'MSFT|20250620|400.00C', 'SPY|20250620|500.00C'],
        'Open Int': [1100, 1250, 520, 1000], # Esta columna se usará para OpenInt_D2
        'Time': ['09:30:00 ET', '09:35:00 ET', '09:32:00 ET', '09:40:00 ET']
    })

    print("--- Probando la cadena de procesamiento completa con datos de ejemplo ---")

    # 1. Procesar datos del Día 1
    processed_day1 = get_last_transactions_day1(df_raw_d1.copy())
    if processed_day1 is not None:
        print("\nDatos procesados del Día 1 (últimas transacciones):")
        print(processed_day1)
    else:
        print("\nError al procesar datos del Día 1.")

    # 2. Procesar datos del Día 2
    processed_day2 = get_first_transaction_open_interest_day2(df_raw_d2.copy())
    if processed_day2 is not None:
        print("\nDatos procesados del Día 2 (primer Open Interest):")
        print(processed_day2)
    else:
        print("\nError al procesar datos del Día 2.")

    # 3. Detectar actividad de dark pool
    if processed_day1 is not None and processed_day2 is not None:
        dark_pool_activity_results = detect_dark_pool_activity(processed_day1, processed_day2)

        if not dark_pool_activity_results.empty:
            print("\nActividad de Dark Pool Detectada:")
            print(dark_pool_activity_results)
        else:
            print("\nNo se detectó actividad de dark pool con los datos de ejemplo.")
    else:
        print("\nNo se pudo proceder a la detección de dark pool debido a errores en el procesamiento previo.")

    # Resultados esperados para el ejemplo:
    #                               Volume_D1  OpenInt_D1  OpenInt_D2  DarkPoolActivity
    # ContractIdentifier
    # AAPL|20250620|235.00P            150.0      1050.0      1100.0             100.0
    # MSFT|20250620|400.00C             50.0       500.0       520.0              30.0
    # GOOG (solo en D1) y SPY (solo en D2) no deberían aparecer en los resultados finales
    # debido al 'inner' join en detect_dark_pool_activity.
