
import streamlit as st
import os
from datetime import datetime
import pytz
from streamlit_autorefresh import st_autorefresh

# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="Social Media Analytics Hub",
    layout="wide"
)

# Auto refresh every 1 min (same as your current logic)
st_autorefresh(interval=60000, key="refresh")

# ---------------- HELPER FUNCTION ----------------
def get_last_updated(file_path):
    if os.path.exists(file_path):
        dubai = pytz.timezone("Asia/Dubai")
        ts = os.path.getmtime(file_path)
        return datetime.fromtimestamp(ts, dubai).strftime("%d-%b-%Y %H:%M:%S")
    return "Not available"

def render_html(file_path, height=1200):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        st.components.v1.html(
            html_content,
            height=height,
            scrolling=True
        )
    else:
        st.warning(f"{file_path} not generated yet.")

# ---------------- SIDEBAR ----------------
st.sidebar.title("📊 Navigation")

page = st.sidebar.radio(
    "Go to",
    ["Home", "Twitter", "LinkedIn"]
)

# ---------------- HOME ----------------
if page == "Home":
    st.title("📊 Social Media Analytics Hub")

    st.markdown("""
    Welcome to the unified analytics dashboard.

    This platform integrates:
    -  Twitter sentiment & engagement
    -  LinkedIn sentiment & engagement

    Use the sidebar to navigate between dashboards.
    """)

# ---------------- TWITTER ----------------
elif page == "Twitter":
    #st.title("🐦 Twitter Dashboard")

    file_path = "X-post_analysis.html"

    last_updated = get_last_updated(file_path)
    #st.caption(f"Last updated: {last_updated} (Dubai Time)")

    render_html(file_path)

# ---------------- LINKEDIN ----------------
elif page == "LinkedIn":
    #st.title("💼 LinkedIn Dashboard")

    file_path = "linkedin_post_analysis.html"

    last_updated = get_last_updated(file_path)
    #st.caption(f"Last updated: {last_updated} (Dubai Time)")

    render_html(file_path)

