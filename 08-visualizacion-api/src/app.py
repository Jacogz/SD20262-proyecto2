"""
Punto 8 - Aplicación Streamlit: Saber 11 (ICFES) - 6 preguntas de negocio.

Consume la API expuesta en API Gateway (Lambda -> S3 refined/api/*.json).
  API_URL=https://xxxx.execute-api.us-east-1.amazonaws.com  streamlit run app.py
Modo local (sin AWS, para desarrollo): LOCAL_DIR=./out streamlit run app.py
"""
import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

st.set_page_config(page_title="Saber 11 · Colombia", page_icon="📊", layout="wide")

API_URL = os.getenv("API_URL", "").rstrip("/")
LOCAL_DIR = os.getenv("LOCAL_DIR", "")


@st.cache_data(ttl=300, show_spinner="Consultando API…")
def get(name):
    if LOCAL_DIR:
        return json.loads((Path(LOCAL_DIR) / f"{name}.json").read_text(encoding="utf-8"))
    r = requests.get(f"{API_URL}/api/{name}", timeout=30)
    r.raise_for_status()
    return r.json()


def bonito(s):
    return s.title().replace("Bogota", "Bogotá D.C.").replace("Narino", "Nariño").replace("Cordoba", "Córdoba").replace("Valle", "Valle del Cauca").replace("Norte Santander", "Norte de Santander")


st.title("📊 Resultados Saber 11 en Colombia")
if not API_URL and not LOCAL_DIR:
    st.error("Configura la variable de entorno API_URL con la URL de API Gateway.")
    st.stop()

try:
    meta = get("meta")
except Exception as e:
    st.error(f"No se pudo conectar con la API: {e}")
    st.stop()

anios = meta["anios"]
c1, c2, c3 = st.columns(3)
c1.metric("Registros institución-jornada", f"{meta['filas']:,}")
c2.metric("Periodos", f"{min(anios)} – {max(anios)}")
c3.metric("Departamentos", meta["departamentos"])
if meta.get("periodos_sinteticos"):
    st.caption("⚠️ Los periodos " + ", ".join(map(str, meta["periodos_sinteticos"])) +
               " usan datos sintéticos generados porque el archivo original del ICFES está corrupto. "
               "Puntaje global en escala 0-500 (promedio ponderado por evaluados).")

tabs = st.tabs(["1 · Mayor/menor puntaje", "2 · Oficial vs no oficial", "3 · Área más débil",
                "4 · Evolución", "5 · Top 10 colegios", "6 · Brecha ciudades"])

# ------------------------------------------------------------------ Q1
with tabs[0]:
    st.subheader("¿Qué departamentos y municipios tienen el mayor y el menor puntaje global promedio?")
    d = get("q1")
    dep = pd.DataFrame(d["departamentos"])
    dep["departamento"] = dep.departamento.map(bonito)
    fig = px.bar(dep.sort_values("puntaje"), x="puntaje", y="departamento", orientation="h",
                 color="puntaje", color_continuous_scale="RdYlGn", height=800)
    fig.update_layout(coloraxis_showscale=False, yaxis_title=None, xaxis_title="Puntaje global promedio")
    st.plotly_chart(fig, width="stretch")
    a, b = st.columns(2)
    for col, key, titulo in [(a, "municipios_top", "🏆 10 municipios con mayor puntaje"),
                             (b, "municipios_bottom", "⬇️ 10 municipios con menor puntaje")]:
        t = pd.DataFrame(d[key])
        t["departamento"] = t.departamento.map(bonito)
        col.markdown(f"**{titulo}**")
        col.dataframe(t, hide_index=True, width="stretch")

# ------------------------------------------------------------------ Q2
with tabs[1]:
    st.subheader("¿Qué diferencia hay entre colegios oficiales y no oficiales, y cómo cambia por zona?")
    d = get("q2")
    nat = pd.DataFrame(d["naturaleza"])
    st.metric("Diferencia (No oficial − Oficial)", f"{d['diferencia_oficial_vs_no_oficial']:+.1f} puntos")
    st.plotly_chart(px.bar(nat, x="naturaleza", y="puntaje", color="naturaleza", text="puntaje"),
                    width="stretch")
    zn = pd.DataFrame(d["zona_naturaleza"])
    if zn.empty:
        st.info("Sin cruce por zona: faltan las tablas RDS en raw/ (sede_educativa, municipio, zona_geografica).")
    else:
        st.plotly_chart(px.bar(zn, x="zona", y="puntaje", color="naturaleza", barmode="group", text="puntaje"),
                        width="stretch")

# ------------------------------------------------------------------ Q3
with tabs[2]:
    st.subheader("¿Qué área tiene el menor desempeño en cada región?")
    t = pd.DataFrame(get("q3")["regiones_areas"])
    pivot = t.pivot(index="region", columns="area", values="puntaje")
    st.plotly_chart(px.imshow(pivot, text_auto=".1f", color_continuous_scale="RdYlGn", aspect="auto"),
                    width="stretch")
    minimos = t[t.es_minimo][["region", "area", "puntaje"]].rename(
        columns={"area": "área con menor desempeño"})
    st.dataframe(minimos, hide_index=True, width="stretch")

# ------------------------------------------------------------------ Q4
with tabs[3]:
    st.subheader("¿Cómo ha evolucionado el puntaje promedio por departamento?")
    d = get("q4")
    ev = pd.DataFrame(d["evolucion"])
    ev["departamento"] = ev.departamento.map(bonito)
    opciones = sorted(ev.departamento.unique())
    sel = st.multiselect("Departamentos", opciones,
                         default=[x for x in ["Antioquia", "Bogotá D.C.", "Valle del Cauca"] if x in opciones])
    nac = pd.DataFrame(d["nacional"]).assign(departamento="NACIONAL")
    plot = pd.concat([ev[ev.departamento.isin(sel)], nac])
    fig = px.line(plot, x="anio", y="puntaje", color="departamento", markers=True)
    fig.update_xaxes(dtick=1)
    st.plotly_chart(fig, width="stretch")

# ------------------------------------------------------------------ Q5
with tabs[4]:
    d = get("q5")
    st.subheader(f"Top 10 colegios por departamento ({d['anio']})")
    t = pd.DataFrame(d["top10"])
    t["departamento"] = t.departamento.map(bonito)
    dep = st.selectbox("Departamento", sorted(t.departamento.unique()))
    sub = t[t.departamento == dep].sort_values("ranking")
    st.plotly_chart(px.bar(sub.sort_values("puntaje"), x="puntaje", y="colegio", orientation="h",
                           color="naturaleza", hover_data=["municipio", "evaluados"]),
                    width="stretch")
    st.dataframe(sub.drop(columns="departamento"), hide_index=True, width="stretch")

# ------------------------------------------------------------------ Q6
with tabs[5]:
    st.subheader("¿Qué tan grande es la brecha entre Bogotá, Medellín, Cali y el resto del país?")
    d = get("q6")
    p = pd.DataFrame(d["puntajes"])
    fig = px.line(p, x="anio", y="puntaje", color="grupo", markers=True)
    fig.update_xaxes(dtick=1)
    st.plotly_chart(fig, width="stretch")
    b = pd.DataFrame(d["brecha_vs_resto"])
    st.plotly_chart(px.bar(b, x="anio", y="brecha", color="ciudad", barmode="group",
                           labels={"brecha": "Puntos sobre el resto del país"}), width="stretch")
