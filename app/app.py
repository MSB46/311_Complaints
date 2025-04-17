import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit.runtime.media_file_storage import MediaFileStorageError

st.set_page_config(layout="wide", page_title="Early dashboard development")
st.title("Forecast Dashboard 1.11")
placeholder_text = '''
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam quis blandit diam. Nam sed felis porttitor, fringilla lacus eget, laoreet mi. Mauris sit amet felis quis metus ultricies dictum. Sed mattis justo non gravida laoreet. Sed nisl lorem, sodales eget nunc eget, posuere sollicitudin nulla. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Praesent pretium sem vitae dignissim interdum. Curabitur id molestie erat.

In cursus metus sem, pharetra porta nisl gravida ut. Sed luctus, nisl sed dapibus sollicitudin, nulla metus dignissim felis, id sagittis nulla lorem a nulla. Maecenas et diam eu enim tincidunt ornare. Vestibulum et odio lectus. Sed viverra volutpat scelerisque. Quisque vestibulum ipsum sit amet mattis auctor. Sed maximus, lorem quis eleifend faucibus, eros lacus ultrices urna, sodales mattis massa mauris eu justo. Suspendisse vitae mauris quis metus sollicitudin vehicula. Sed vehicula pulvinar est nec accumsan. Mauris tempus, neque egestas malesuada consequat, quam lacus lacinia libero, et elementum orci odio sit amet quam. Morbi quis mauris id erat euismod cursus volutpat ac turpis. In lobortis varius tellus, eget sollicitudin tellus mollis id.
'''

col1, col2 = st.columns(2)

with col1:
    st.title("Test HTML")
    try:
        html_path = r"shared/311_noise_forecast_plot.html"
        with open(html_path, 'r', errors="ignore") as f:
            html_data = f.read()
        st.components.v1.html(html_data, height=900)
    except (MediaFileStorageError, FileNotFoundError):
        st.write("No data available yet...")

with col2:
    st.markdown('''
    ##
    ## Test Markdown
    ''')
    st.write(placeholder_text)
# st.markdown(html_data, unsafe_allow_html=True)

# update every hour
MINUTE_UPDATE = 60
MINUTE_UPDATE = 1
st_autorefresh(interval=MINUTE_UPDATE * 60 * 1000)