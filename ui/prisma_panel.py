import streamlit as st
import plotly.graph_objects as go

from storage.repository import ArticleRepository

repo = ArticleRepository()


def render_prisma_diagram(review_id: int):

    counts = repo.get_screening_counts(review_id)

    identified = counts["total"]
    screened = counts["total"]
    excluded = counts["excluded"]
    included = counts["included"]

    fig = go.Figure()

    fig.add_annotation(
        x=0.5,
        y=0.9,
        text=f"Records identified: {identified}",
        showarrow=False,
    )

    fig.add_annotation(
        x=0.5,
        y=0.7,
        text=f"Records screened: {screened}",
        showarrow=False,
    )

    fig.add_annotation(
        x=0.2,
        y=0.5,
        text=f"Excluded: {excluded}",
        showarrow=False,
    )

    fig.add_annotation(
        x=0.8,
        y=0.5,
        text=f"Included: {included}",
        showarrow=False,
    )

    fig.update_layout(
        height=400,
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )

    st.plotly_chart(fig)