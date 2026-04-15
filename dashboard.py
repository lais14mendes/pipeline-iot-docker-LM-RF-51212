"""
dashboard.py
Dashboard interativo de temperaturas IoT com Streamlit + Plotly
Disciplina: Disruptive Architectures: IoT, Big Data e IA - UNIFECAF
Autora: Lais Goncalves Mendes | RA: 51212
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard IoT - Temperaturas",
    page_icon="🌡️",
    layout="wide"
)

# ─────────────────────────────────────────────
# CONEXÃO COM O BANCO — via .env
# ─────────────────────────────────────────────
load_dotenv()

DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "iot1234")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "iot_pipeline")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Views permitidas (proteção contra SQL injection)
VALID_VIEWS = [
    "avg_temp_por_dispositivo",
    "leituras_por_hora",
    "temp_max_min_por_dia",
]

@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL)

@st.cache_data(ttl=300, show_spinner=False)
def load_data(view_name: str) -> pd.DataFrame:
    """Carrega dados de uma view do PostgreSQL com validação."""
    if view_name not in VALID_VIEWS:
        raise ValueError(f"View inválida: {view_name}")
    engine = get_engine()
    return pd.read_sql(f"SELECT * FROM {view_name}", engine)

@st.cache_data(ttl=300, show_spinner=False)
def get_total_registros() -> int:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM temperature_readings"))
        return result.scalar()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.title("🌡️ Dashboard de Temperaturas IoT")
st.markdown(
    "**Disciplina:** Disruptive Architectures: IoT, Big Data e IA · UNIFECAF  \n"
    "**Autora:** Lais Goncalves Mendes | RA: 51212"
)
st.divider()

try:
    with st.spinner("Carregando dados do banco..."):
        df_avg  = load_data("avg_temp_por_dispositivo")
        df_hora = load_data("leituras_por_hora")
        df_dia  = load_data("temp_max_min_por_dia")
        total   = get_total_registros()

    # ── Validação de dados vazios ─────────────────────────────────────────
    if df_avg.empty or df_hora.empty or df_dia.empty:
        st.warning("⚠️ Banco de dados vazio. Execute o load_data.py primeiro.")
        st.code("python src/load_data.py")
        st.stop()

    # ─────────────────────────────────────────
    # SIDEBAR — FILTROS INTERATIVOS
    # ─────────────────────────────────────────
    st.sidebar.header("🔎 Filtros")
    st.sidebar.markdown("---")

    # Filtro por dispositivo
    todos_dispositivos = df_avg["device_id"].unique().tolist()
    dispositivos_sel = st.sidebar.multiselect(
        "📡 Dispositivos:",
        options=todos_dispositivos,
        default=todos_dispositivos,
        help="Selecione os dispositivos IoT para exibir no dashboard"
    )

    # Filtro por período
    df_dia["data"] = pd.to_datetime(df_dia["data"])
    data_min = df_dia["data"].min().date()
    data_max = df_dia["data"].max().date()

    periodo = st.sidebar.date_input(
        "📅 Período:",
        value=(data_min, data_max),
        min_value=data_min,
        max_value=data_max,
        help="Filtre o intervalo de datas para os gráficos de amplitude"
    )

    st.sidebar.markdown("---")
    st.sidebar.info(f"📊 **{total:,}** leituras no banco")

    # Aplicar filtros
    if dispositivos_sel:
        df_avg_filtrado = df_avg[df_avg["device_id"].isin(dispositivos_sel)]
    else:
        df_avg_filtrado = df_avg

    if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
        data_ini = pd.Timestamp(periodo[0])
        data_fim = pd.Timestamp(periodo[1])
        df_dia_filtrado = df_dia[
            (df_dia["data"] >= data_ini) & (df_dia["data"] <= data_fim)
        ]
    else:
        df_dia_filtrado = df_dia

    # ─────────────────────────────────────────
    # MÉTRICAS RÁPIDAS
    # ─────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📦 Total de Leituras",       f"{total:,}")
    col2.metric("🌡️ Temp. Média Geral",       f"{df_avg_filtrado['avg_temp'].mean():.1f} °C")
    col3.metric("🔴 Temp. Máxima Registrada", f"{df_dia_filtrado['temp_max'].max():.1f} °C")
    col4.metric("🔵 Temp. Mínima Registrada", f"{df_dia_filtrado['temp_min'].min():.1f} °C")

    st.divider()

    # ─────────────────────────────────────────
    # GRÁFICO 1 — Média por dispositivo
    # ─────────────────────────────────────────
    st.subheader("📊 Gráfico 1 — Média de Temperatura por Dispositivo")
    st.caption("View: `avg_temp_por_dispositivo` | Temperatura média de cada sensor IoT.")

    if df_avg_filtrado.empty:
        st.warning("Nenhum dispositivo selecionado. Use o filtro na barra lateral.")
    else:
        fig1 = px.bar(
            df_avg_filtrado.sort_values("avg_temp", ascending=False),
            x="device_id",
            y="avg_temp",
            color="avg_temp",
            color_continuous_scale="RdYlBu_r",
            labels={"device_id": "Dispositivo", "avg_temp": "Temperatura Média (°C)"},
            title="Temperatura Média por Dispositivo IoT",
            text_auto=".1f"
        )
        fig1.update_layout(
            xaxis_tickangle=-45,
            coloraxis_showscale=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig1, use_container_width=True)

    st.divider()

    # ─────────────────────────────────────────
    # GRÁFICO 2 — Leituras por hora
    # ─────────────────────────────────────────
    st.subheader("🕐 Gráfico 2 — Distribuição de Leituras por Hora do Dia")
    st.caption("View: `leituras_por_hora` | Quantidade de leituras em cada hora do dia.")

    fig2 = px.line(
        df_hora,
        x="hora",
        y="contagem",
        markers=True,
        labels={"hora": "Hora do Dia", "contagem": "Nº de Leituras"},
        title="Distribuição de Leituras ao Longo do Dia",
        line_shape="spline"
    )
    fig2.update_traces(
        line_color="#E74C3C",
        marker_color="#C0392B",
        marker_size=8
    )
    fig2.update_xaxes(
        tickvals=list(range(0, 24)),
        ticktext=[f"{h:02d}h" for h in range(24)]
    )
    fig2.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # ─────────────────────────────────────────
    # GRÁFICO 3 — Amplitude térmica diária
    # ─────────────────────────────────────────
    st.subheader("📈 Gráfico 3 — Amplitude Térmica Diária")
    st.caption("View: `temp_max_min_por_dia` | Máxima, mínima e média diária de temperatura.")

    if df_dia_filtrado.empty:
        st.warning("Nenhum dado no período selecionado.")
    else:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=df_dia_filtrado["data"], y=df_dia_filtrado["temp_max"],
            mode="lines+markers", name="Máxima",
            line=dict(color="#E74C3C", width=2), fill=None
        ))
        fig3.add_trace(go.Scatter(
            x=df_dia_filtrado["data"], y=df_dia_filtrado["temp_min"],
            mode="lines+markers", name="Mínima",
            line=dict(color="#3498DB", width=2),
            fill="tonexty", fillcolor="rgba(52,152,219,0.1)"
        ))
        fig3.add_trace(go.Scatter(
            x=df_dia_filtrado["data"], y=df_dia_filtrado["temp_media"],
            mode="lines", name="Média",
            line=dict(color="#2ECC71", width=1.5, dash="dash")
        ))
        fig3.update_layout(
            title="Amplitude Térmica Diária (Máx / Média / Mín)",
            xaxis_title="Data",
            yaxis_title="Temperatura (°C)",
            legend_title="Série",
            hovermode="x unified",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    # ─────────────────────────────────────────
    # INSIGHTS
    # ─────────────────────────────────────────
    st.subheader("💡 Principais Insights")

    hora_pico = int(df_hora.loc[df_hora["contagem"].idxmax(), "hora"])
    hora_pico_val = int(df_hora.loc[df_hora["contagem"].idxmax(), "contagem"])
    amplitude = (df_dia["temp_max"] - df_dia["temp_min"]).mean()
    temp_critica = df_avg["avg_temp"].max()
    device_critico = df_avg.loc[df_avg["avg_temp"].idxmax(), "device_id"]

    col_a, col_b, col_c = st.columns(3)
    col_a.info(
        f"**⏰ Horário de pico:** {hora_pico:02d}h\n\n"
        f"{hora_pico_val:,} leituras nesse horário. "
        f"Indica maior demanda de monitoramento no período."
    )
    col_b.info(
        f"**📏 Amplitude média diária:** {amplitude:.1f} °C\n\n"
        f"Variação entre máxima e mínima diária. "
        f"Indica instabilidade térmica no ambiente."
    )
    col_c.warning(
        f"**🚨 Dispositivo crítico:** {device_critico}\n\n"
        f"Maior média registrada: **{temp_critica:.1f} °C**. "
        f"Recomenda-se inspeção preventiva."
    )

    # ─────────────────────────────────────────
    # RODAPÉ
    # ─────────────────────────────────────────
    st.divider()
    st.markdown(
        "<div style='text-align:center; color:gray; font-size:12px;'>"
        "UNIFECAF · Disruptive Architectures: IoT, Big Data e IA · 2025 · "
        "Lais Goncalves Mendes | RA 51212"
        "</div>",
        unsafe_allow_html=True
    )

except Exception as e:
    st.error(f"❌ Erro ao conectar ao banco de dados: {e}")
    st.info("Verifique se o container Docker está rodando e se o load_data.py foi executado.")
    st.code("docker run --name postgres-iot -e POSTGRES_PASSWORD=iot1234 -e POSTGRES_DB=iot_pipeline -p 5432:5432 -d postgres")
    st.code("python src/load_data.py")
