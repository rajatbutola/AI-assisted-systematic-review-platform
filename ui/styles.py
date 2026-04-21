# ui/styles.py  — v6 Nexus Scholar (White top bar + Toggle FIXED)

import streamlit as st


def inject_styles() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap');

/* Remove white top rectangle completely */
.stApp {
  background: linear-gradient(180deg, #F8F7F4 0%, #F0EDE6 100%) !important;
  padding-top: 0 !important;
  margin-top: 0 !important;
}

.block-container {
  padding-top: 0.5rem !important;
  max-width: 1300px !important;
}

/* ── THIS IS THE ONLY ADDITION — removes the white header bar ── */
header[data-testid="stHeader"] {
  background: transparent !important;
  box-shadow: none !important;
  border: none !important;
}

/* Sidebar styling */
section[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #0A1428 0%, #11203F 100%) !important;
  border-right: 4px solid #E8C45A !important;
  color: #CBD5E1 !important;
}

section[data-testid="stSidebar"] * {
  color: #CBD5E1 !important;
}

/* New Review expander */
section[data-testid="stSidebar"] .streamlit-expanderHeader {
  background: rgba(255,255,255,0.1) !important;
  border: 1px solid rgba(232,196,90,0.4) !important;
  border-radius: 12px !important;
  color: #E8C45A !important;
  font-weight: 600;
}

/* Page Header - Glowing Orb */
.sr-page-header .sr-icon-box {
  width: 56px; height: 56px;
  background: linear-gradient(135deg, #00F5D4, #E8C45A);
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 1.8rem;
  box-shadow: 0 0 0 8px rgba(0,245,212,0.2), 0 0 30px -5px rgba(232,196,90,0.7);
}

/* Metric cards */
[data-testid="metric-container"] {
  background: rgba(255,255,255,0.92) !important;
  border-radius: 16px !important;
  box-shadow: 0 10px 30px -10px rgba(10,20,40,0.15) !important;
}

/* Tabs */
.stTabs [aria-selected="true"] {
  background: linear-gradient(90deg, #0A1428, #1C2E55) !important;
  color: white !important;
}

/* Primary buttons */
.stButton > button[kind="primary"] {
  background: linear-gradient(90deg, #00F5D4, #00C4A8) !important;
  color: #0A1428 !important;
  font-weight: 700;
  border-radius: 9999px !important;
}

/* Hide default Streamlit chrome */
#MainMenu, footer, [data-testid="stToolbar"], [data-testid="stDecoration"] {
  visibility: hidden;
}
</style>
"""