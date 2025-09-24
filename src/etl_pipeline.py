
import os
import re
import urllib
import logging
import pandas as pd
from sqlalchemy import create_engine, inspect

# ---------- Configuración ----------
CSV_DIR = "./csvs"
OUTPUT_DIR = "./output_csvs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=DESKTOP-HBE6BOE\\MSSQLSERVER01;"
    "DATABASE=OpinionesDB;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
ENGINE_URL = f"mssql+pyodbc:///?odbc_connect={params}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Funciones comunes ----------
def read_csv_safe(name):
    path = os.path.join(CSV_DIR, name)
    if os.path.exists(path):
        return pd.read_csv(path, low_memory=False)
    logging.warning(f"Archivo {name} no encontrado, se ignora")
    return pd.DataFrame()

def normalize_text(s):
    if pd.isna(s):
        return ''
    s = str(s).strip()
    return re.sub(r'\s+', ' ', s)

def parse_date(s):
    if pd.isna(s):
        return None
    return pd.to_datetime(s, errors='coerce')

def export_to_excel_csv(df, table_name):
    """Exporta DataFrame a CSV listo para Excel"""
    if df.empty:
        return
    output_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
    df.to_csv(output_path, sep=';', index=False, encoding='utf-8-sig')
    logging.info(f"CSV para Excel generado: {output_path}")

# ---------- Limpieza por tipo ----------
def clean_products(df):
    if df.empty: return df
    df = df.copy()
    df['SKU'] = df['IdProducto'] if 'IdProducto' in df.columns else pd.Series(['']*len(df))
    df['Name'] = df['Nombre'].apply(normalize_text) if 'Nombre' in df.columns else pd.Series(['']*len(df))
    df['Category'] = df['Categoría'].apply(normalize_text) if 'Categoría' in df.columns else pd.Series(['']*len(df))
    df['Price'] = df.get('Precio', 0.0).apply(lambda x: float(x) if pd.notna(x) else 0.0)
    return df[['SKU','Name','Category','Price']].drop_duplicates(subset=['SKU'])

def clean_clients(df):
    if df.empty: return df
    df = df.copy()
    df['ExternalId'] = df['IdCliente'] if 'IdCliente' in df.columns else pd.Series(['']*len(df))
    df['FullName'] = df['Nombre'].apply(normalize_text) if 'Nombre' in df.columns else pd.Series(['']*len(df))
    df['Email'] = df['Email'].apply(normalize_text) if 'Email' in df.columns else pd.Series(['']*len(df))
    return df[['ExternalId','FullName','Email']].drop_duplicates(subset=['ExternalId'])

def clean_sources(df):
    if df.empty: return df
    df = df.copy()
    df['Name'] = df['TipoFuente'].apply(normalize_text) if 'TipoFuente' in df.columns else pd.Series(['']*len(df))
    df['Type'] = df['TipoFuente'].apply(normalize_text) if 'TipoFuente' in df.columns else pd.Series(['']*len(df))
    df['Url'] = ''  # No tienes URL en tu CSV
    return df[['Name','Type','Url']].drop_duplicates(subset=['Name'])

def clean_surveys(df, engine):
    if df.empty: return df
    df = df.copy()
    df['SurveyDate'] = df['Fecha'].apply(parse_date) if 'Fecha' in df.columns else pd.Series([None]*len(df))
    df['Comment'] = df['Comentario'].apply(normalize_text) if 'Comentario' in df.columns else pd.Series(['']*len(df))
    df['Rating'] = pd.to_numeric(df.get('PuntajeSatisfacción',0), errors='coerce').fillna(0)

    # Mapear SKU a ProductId
    products_df = pd.read_sql("SELECT ProductId, SKU FROM Products", engine)
    sku_map = dict(zip(products_df['SKU'], products_df['ProductId']))
    df['ProductId'] = df['IdProducto'].map(sku_map)

    # Mapear IdCliente a CustomerId
    customers_df = pd.read_sql("SELECT CustomerId, ExternalId FROM Customers", engine)
    customer_map = dict(zip(customers_df['ExternalId'], customers_df['CustomerId']))
    df['CustomerId'] = df['IdCliente'].map(customer_map)

    # Mapear Fuente a SourceId
    sources_df = pd.read_sql("SELECT SourceId, Name FROM Sources", engine)
    source_map = dict(zip(sources_df['Name'], sources_df['SourceId']))
    df['SourceId'] = df['Fuente'].map(source_map)

    return df[['SurveyDate','ProductId','CustomerId','Rating','Comment','SourceId']]

def clean_social_comments(df, engine):
    if df.empty: return df
    df = df.copy()
    df['CommentDate'] = df['Fecha'].apply(parse_date) if 'Fecha' in df.columns else pd.Series([None]*len(df))
    df['Body'] = df['Comentario'].apply(normalize_text) if 'Comentario' in df.columns else pd.Series(['']*len(df))
    df['UserHandle'] = df['IdCliente'] if 'IdCliente' in df.columns else pd.Series(['']*len(df))

    # Mapear SKU a ProductId
    products_df = pd.read_sql("SELECT ProductId, SKU FROM Products", engine)
    sku_map = dict(zip(products_df['SKU'], products_df['ProductId']))
    df['ProductId'] = df['IdProducto'].map(sku_map)

    # Mapear Fuente a SourceId
    sources_df = pd.read_sql("SELECT SourceId, Name FROM Sources", engine)
    source_map = dict(zip(sources_df['Name'], sources_df['SourceId']))
    df['SourceId'] = df['Fuente'].map(source_map)

    return df[['UserHandle','Body','CommentDate','ProductId','SourceId']]

def clean_web_reviews(df, engine):
    if df.empty: return df
    df = df.copy()
    df['ReviewDate'] = df['Fecha'].apply(parse_date) if 'Fecha' in df.columns else pd.Series([None]*len(df))
    df['Body'] = df['Comentario'].apply(normalize_text) if 'Comentario' in df.columns else pd.Series(['']*len(df))
    df['ReviewerName'] = df['IdCliente'] if 'IdCliente' in df.columns else pd.Series(['']*len(df))
    df['Rating'] = pd.to_numeric(df.get('Rating',0), errors='coerce').fillna(0)

    # Mapear SKU a ProductId
    products_df = pd.read_sql("SELECT ProductId, SKU FROM Products", engine)
    sku_map = dict(zip(products_df['SKU'], products_df['ProductId']))
    df['ProductId'] = df['IdProducto'].map(sku_map)

    # Mapear Fuente a SourceId solo si existe
    sources_df = pd.read_sql("SELECT SourceId, Name FROM Sources", engine)
    source_map = dict(zip(sources_df['Name'], sources_df['SourceId']))
    if 'Fuente' in df.columns:
        df['SourceId'] = df['Fuente'].map(source_map)
    else:
        df['SourceId'] = None  # Asignar None o un SourceId por defecto

    return df[['ReviewerName','Body','ReviewDate','Rating','ProductId','SourceId']]


# ---------- Pipeline ETL ----------
def main():
    engine = create_engine(ENGINE_URL, fast_executemany=True)
    inspector = inspect(engine)

    # Se asegura de cargar Sources primero
    sources_df = read_csv_safe("fuente_datos.csv")
    if not sources_df.empty:
        df_sources = clean_sources(sources_df)
        if not df_sources.empty:
            df_sources.to_sql("Sources", engine, if_exists='append', index=False)
            export_to_excel_csv(df_sources, "Sources")

    datasets = [
        ('products.csv', clean_products, 'Products', False),
        ('clients.csv', clean_clients, 'Customers', False),
        ('surveys_part1.csv', clean_surveys, 'Surveys', True),
        ('social_comments.csv', clean_social_comments, 'SocialComments', True),
        ('web_reviews.csv', clean_web_reviews, 'WebReviews', True),
    ]

    for file_name, clean_func, table_name, needs_engine in datasets:
        try:
            df = read_csv_safe(file_name)
            if df.empty:
                logging.info(f"No hay datos para {table_name}, se salta")
                continue

            # Evitar reinsertar productos si ya existen
            if table_name == "Products" and inspector.has_table(table_name):
                logging.info("Productos ya existen, se omite inserción")
            else:
                df_clean = clean_func(df, engine) if needs_engine else clean_func(df)
                if not df_clean.empty:
                    df_clean.to_sql(table_name, engine, if_exists='append', index=False)
                    logging.info(f"{table_name} insertados: {len(df_clean)}")
                    export_to_excel_csv(df_clean, table_name)
                else:
                    logging.info(f"No hay datos limpios para {table_name}, se salta")
        except Exception as e:
            logging.error(f"Error procesando {file_name}: {e}")

if __name__ == "__main__":
    main()
