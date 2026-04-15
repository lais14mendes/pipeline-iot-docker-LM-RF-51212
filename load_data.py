"""
load_data.py
Pipeline de Dados IoT - Leitura do CSV e inserção no PostgreSQL
Disciplina: Disruptive Architectures: IoT, Big Data e IA - UNIFECAF
Autora: Lais Gonçalves Mendes | RA: 51212
"""

import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DE LOG
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DE CONEXÃO — via .env
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "iot1234")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "iot_pipeline")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Caminho para o CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "..", "data", "IOT-temp.csv")

# Limites de temperatura considerados válidos (°C)
TEMP_MIN_VALIDA =  0.0
TEMP_MAX_VALIDA = 80.0


# ─────────────────────────────────────────────────────────────────────────────
def criar_tabela(engine):
    """Cria a tabela principal se não existir."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS temperature_readings (
                id          SERIAL PRIMARY KEY,
                device_id   VARCHAR(50)  NOT NULL,
                noted_date  TIMESTAMP    NOT NULL,
                temp        FLOAT        NOT NULL,
                location    VARCHAR(10)
            );
        """))
        conn.commit()
    log.info("Tabela 'temperature_readings' verificada/criada.")


# ─────────────────────────────────────────────────────────────────────────────
def criar_views(engine):
    """Cria (ou atualiza) as 3 views SQL analíticas."""
    views = {
        "avg_temp_por_dispositivo": """
            CREATE OR REPLACE VIEW avg_temp_por_dispositivo AS
            SELECT
                device_id,
                ROUND(AVG(temp)::numeric, 2) AS avg_temp,
                COUNT(*)                     AS total_leituras
            FROM temperature_readings
            GROUP BY device_id
            ORDER BY avg_temp DESC;
        """,
        "leituras_por_hora": """
            CREATE OR REPLACE VIEW leituras_por_hora AS
            SELECT
                EXTRACT(HOUR FROM noted_date)::int AS hora,
                COUNT(*)                            AS contagem,
                ROUND(AVG(temp)::numeric, 2)        AS temp_media
            FROM temperature_readings
            GROUP BY hora
            ORDER BY hora;
        """,
        "temp_max_min_por_dia": """
            CREATE OR REPLACE VIEW temp_max_min_por_dia AS
            SELECT
                DATE(noted_date)             AS data,
                ROUND(MAX(temp)::numeric, 2) AS temp_max,
                ROUND(MIN(temp)::numeric, 2) AS temp_min,
                ROUND(AVG(temp)::numeric, 2) AS temp_media
            FROM temperature_readings
            GROUP BY DATE(noted_date)
            ORDER BY data;
        """,
    }
    with engine.connect() as conn:
        for nome, sql in views.items():
            conn.execute(text(sql))
            conn.commit()
            log.info(f"View '{nome}' criada/atualizada.")


# ─────────────────────────────────────────────────────────────────────────────
def tabela_ja_tem_dados(engine) -> bool:
    """Verifica se a tabela já possui registros para evitar duplicação."""
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM temperature_readings")).scalar()
    if total > 0:
        log.warning(f"Tabela já contém {total:,} registros. Pulando inserção.")
        log.warning("Para recarregar: TRUNCATE TABLE temperature_readings;")
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
def carregar_csv(engine):
    """Lê o CSV, trata e valida os dados, e insere no PostgreSQL."""

    if tabela_ja_tem_dados(engine):
        return

    if not os.path.exists(CSV_PATH):
        log.error(f"CSV não encontrado em: {CSV_PATH}")
        log.error("Baixe o IOT-temp.csv do Kaggle e coloque na pasta /data")
        sys.exit(1)

    log.info(f"Lendo CSV em: {os.path.abspath(CSV_PATH)}")
    df = pd.read_csv(CSV_PATH)
    total_bruto = len(df)
    log.info(f"{total_bruto:,} registros carregados. Colunas: {list(df.columns)}")

    # Renomeação
    df.rename(columns={
        "room_id/id": "device_id",
        "noted_date": "noted_date",
        "temp":       "temp",
        "out/in":     "location",
    }, inplace=True)

    if "id" in df.columns:
        df.drop(columns=["id"], inplace=True)

    for col in ["device_id", "noted_date", "temp", "location"]:
        if col not in df.columns:
            df[col] = None

    df = df[["device_id", "noted_date", "temp", "location"]]

    # Conversão de tipos
    df["noted_date"] = pd.to_datetime(df["noted_date"], dayfirst=True, errors="coerce")
    df["temp"]       = pd.to_numeric(df["temp"], errors="coerce")

    # Limpeza de nulos
    antes_nulos = len(df)
    df.dropna(subset=["device_id", "noted_date", "temp"], inplace=True)
    removidos_nulos = antes_nulos - len(df)

    # Remoção de outliers
    antes_outliers = len(df)
    df = df[(df["temp"] >= TEMP_MIN_VALIDA) & (df["temp"] <= TEMP_MAX_VALIDA)]
    removidos_outliers = antes_outliers - len(df)

    # Validação de location
    df["location"] = df["location"].where(df["location"].isin({"In", "Out"}), other=None)

    # Remoção de duplicatas
    antes_dup = len(df)
    df.drop_duplicates(subset=["device_id", "noted_date"], inplace=True)
    removidos_dup = antes_dup - len(df)

    total_valido = len(df)

    # Relatório
    log.info("=" * 50)
    log.info("  RELATORIO DE LIMPEZA")
    log.info(f"  Registros brutos:         {total_bruto:>8,}")
    log.info(f"  Removidos (nulos):        {removidos_nulos:>8,}")
    log.info(f"  Removidos (outliers):     {removidos_outliers:>8,}")
    log.info(f"  Removidos (duplicatas):   {removidos_dup:>8,}")
    log.info(f"  Registros validos finais: {total_valido:>8,}")
    log.info(f"  Taxa de aproveitamento:   {(total_valido/total_bruto*100):.1f}%")
    log.info("=" * 50)

    # Inserção
    log.info("Inserindo dados no PostgreSQL em lotes de 1.000...")
    df.to_sql(
        "temperature_readings",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    log.info(f"{total_valido:,} registros inseridos com sucesso!")


# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  PIPELINE DE DADOS IoT — UNIFECAF")
    log.info("  Lais Goncalves Mendes | RA: 51212")
    log.info("=" * 55)

    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Conexao com PostgreSQL estabelecida com sucesso.")
    except Exception as e:
        log.error(f"Nao foi possivel conectar ao banco: {e}")
        log.error("Verifique se o container Docker esta rodando.")
        sys.exit(1)

    criar_tabela(engine)
    criar_views(engine)
    carregar_csv(engine)

    log.info("=" * 55)
    log.info("  PIPELINE CONCLUIDO!")
    log.info("  Execute: streamlit run src/dashboard.py")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
