import pandas as pd

df_caracteristicas_equipos = pd.read_csv('data\Caracteristicas_Equipos.csv')
df_historicos_ordenes = pd.read_csv('data\Historicos_Ordenes.csv')
df_registros_condiciones = pd.read_csv('data\Registros_Condiciones.csv')


def convertir_fecha_a_datetime(df, columna):
    df[columna] = pd.to_datetime(df[columna])
    return df

def cantidad_nulos_por_cada_columna(df, nombre_df):
    print ("--------------------------------------------------")
    print (f'Cantidad de nulos por cada columna de {nombre_df}')
    print (df.isnull().sum().to_frame('Total_Nulos').T)

def mostrar_duplicados_por_columna(df, nombre_columna, nombre_df):
    print ("--------------------------------------------------")
    duplicados = df[df.duplicated(subset=[nombre_columna])]
    print (f'Duplicados en la columna {nombre_columna} de {nombre_df}')
    print (duplicados)
    print (f'Cantidad de duplicados: {len(duplicados)}')
    # Eliminar duplicados y mantener la primera ocurrencia
    df_sin_duplicados = df.drop_duplicates(subset=[nombre_columna], keep='first')
    print(f'\nSe eliminaron {len(df) - len(df_sin_duplicados)} registros duplicados')
    return df_sin_duplicados


df_historicos_ordenes = convertir_fecha_a_datetime(df_historicos_ordenes, 'Fecha')
df_registros_condiciones = convertir_fecha_a_datetime(df_registros_condiciones, 'Fecha')

cantidad_nulos_por_cada_columna(df_caracteristicas_equipos, 'df_caracteristicas_equipos')
cantidad_nulos_por_cada_columna(df_historicos_ordenes, 'df_historicos_ordenes')
cantidad_nulos_por_cada_columna(df_registros_condiciones, 'df_registros_condiciones')

mostrar_duplicados_por_columna(df_caracteristicas_equipos, 'ID_Equipo', 'df_caracteristicas_equipos')
mostrar_duplicados_por_columna(df_historicos_ordenes, 'ID_Orden', 'df_historicos_ordenes')
mostrar_duplicados_por_columna(df_registros_condiciones, 'ID_Registro', 'df_registros_condiciones')


# Hacemos el merge usando 'ID_Equipo' y eliminamos duplicados
df_trabajo_modelo = df_historicos_ordenes.merge(
    df_caracteristicas_equipos[['ID_Equipo', 'Horas_Recomendadas_Revision']],
    on='ID_Equipo',
    how='left'  # Utilizamos left join para mantener todos los registros del primer DataFrame
).drop_duplicates(subset=['ID_Equipo', 'Fecha'])

# Ordenamos el DataFrame por ID_Equipo y Fecha
df_trabajo_modelo = df_trabajo_modelo.sort_values(['ID_Equipo', 'Fecha'])

# Calculamos la diferencia de tiempo con la orden anterior para cada equipo
df_trabajo_modelo['Horas_Desde_Ultima_Orden'] = df_trabajo_modelo.groupby('ID_Equipo')['Fecha'].diff().dt.total_seconds() / 3600

# Reemplazamos los NaN (que corresponden a la primera orden de cada equipo) con 0
df_trabajo_modelo['Horas_Desde_Ultima_Orden'] = df_trabajo_modelo['Horas_Desde_Ultima_Orden'].fillna(0)

# Preparamos el DataFrame de registros de condiciones seleccionando solo las columnas necesarias
df_condiciones_reducido = df_registros_condiciones[['Fecha', 'ID_Equipo', 'Temperatura_C', 'Vibracion_mm_s', 'Horas_Operativas']]

# Realizamos un merge asíncrono para encontrar los registros más cercanos en tiempo
df_trabajo_modelo = pd.merge_asof(
    df_trabajo_modelo.sort_values('Fecha'),
    df_condiciones_reducido.sort_values('Fecha'),
    by='ID_Equipo',
    on='Fecha',
    direction='nearest'
)
print (df_trabajo_modelo.head())










