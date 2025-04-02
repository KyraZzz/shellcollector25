import streamlit as st
import pandas as pd
import plotly.express as px

st.title("Financial Dashboard")

st.sidebar.title("Settings")

uploaded_file = st.sidebar.file_uploader("Upload CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, delimiter=";")

    if 'product' not in df.columns:
        st.error("The CSV must contain a 'product' column.")
    else:
        products = df['product'].unique()
        selected_product = st.sidebar.selectbox("Select Product", products)

        product_df = df[df['product'] == selected_product]

        columns = product_df.columns.tolist()

        x_axis = st.sidebar.selectbox("Select X-axis", columns)
        y_axis = st.sidebar.selectbox("Select Y-axis", columns)

        product_df = product_df.sort_values(by=x_axis)

        fig = px.line(
            product_df,
            x=x_axis,
            y=y_axis,
            title=f"{y_axis} vs {x_axis} for {selected_product}"
        )

        fig.update_layout(
            xaxis_title=x_axis,
            yaxis_title=y_axis,
            width=1000,
            height=700,
            dragmode='pan',
            xaxis=dict(
                tickmode='auto',
                nticks=50,
                autorange=True
            )
        )

        config = {
            'displayModeBar': True,
            'modeBarButtonsToRemove': [],
            'scrollZoom': True
        }

        st.plotly_chart(fig, config=config)

else:
    st.write("Please upload a CSV file to proceed.")
