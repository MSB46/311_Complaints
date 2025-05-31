import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh
from streamlit.runtime.media_file_storage import MediaFileStorageError

st.set_page_config(layout="wide", page_title="311 Noise Analysis")
css = '''
<style>
section.main > div:has(~ footer ) {
    padding-bottom: 5px;
}
* { color: white !important; }
</style>
'''

DEFAULT_HEIGHT = 460
with st.container():
    # HEADER
    st.markdown(
        '''
        # Racket Crackdown  
        ## A forecast and analysis on 311 noise complaints filed in New York City.
        ''')
    st.markdown(
        '''
        ''')
    with open("shared/311_noise_forecast_plot.html",'r', errors="ignore") as f:
        html_data = f.read()
    st.components.v1.html(html_data, height=DEFAULT_HEIGHT)

    # SECTION (ISSUE + OBJECTIVE)
    with st.container():
        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown(
                '''
                ### The Issue
                **How should agencies responsible for noise complaints allocate their resources to handle issues more efficiently?**

                For the supposed "*city that never sleeps*", noise remains a persistent issue for residents. For the police department and other similar agencies intended to investigate such problems, carefully stationing workers based on various factors can prove to be effective for prioritizing other disturbances within the city.  
                '''
            )
        with col2:
            st.markdown(
                '''
                ### The Objective 
                *The following includes some guidelines intended to get a better idea on what said factors are.*
                * Assess the **relationship** between various **noises** to the **complaint's time and location** 

                * **Forecast** the amount of complaints in the next day.

                * Determine the **locations** associated with various **noise complaints** made in each **borough**.

                * Identify any addition key factors that correlate to noise complaint frequency
                '''
            )

    st.write('\n')
    # SECTION (DAY OF WEEK + TIME OF DAY)
    st.title("Findings")
    with st.container(border=True, height=1375):
        st.markdown('## Time and Day')
        with open("shared/hour_subplot.html", 'r', errors="ignore") as f:
            html_data_2a = f.read()
        st.components.v1.html(html_data_2a, height=DEFAULT_HEIGHT)

        col1, col2 = st.columns(2)
        with col1:
            with open("shared/hour_avg_bar.html", 'r', errors="ignore") as f:
                html_data_2b = f.read()
            st.components.v1.html(html_data_2b, height=DEFAULT_HEIGHT)
        with col2:
            with open("shared/dayofweek_avg_bar.html", 'r', errors="ignore") as f:
                html_data_2c = f.read()
            st.components.v1.html(html_data_2c, height=DEFAULT_HEIGHT)


        st.markdown(
            '''                
            * Out of the five weekdays, **Friday** has the **highest volume of noise complaints** on average with at least a 10% difference from Monday, the second highest. Both are the only weekdays that exceed an average of 1000 complaints

            * There doesn't seem to be a significant difference between the other weekdays

            * On average, **weekends** are noticeably higher in noise complaints than weekdays with more than a 35% difference between Friday and either weekend.

            * For every day of the week there is a **fall-off in complaints towards the early morning hours** (From 12AM to 5AM/6AM for weekdays and weekends respectively). 

            * Each weekday with the exception of Friday **peaks in total complaints at the 22nd hour (10PM)**. 

            * **Friday** is the **only weekday** where total complaints peak at the **23rd hour** (11PM). Likely from more **people staying up for the weekend break**. 

            * **Sunday** is the only day where the total **complaints peak at midnight**. Also a possible result of more people staying up late for the weekend. 
            '''
        )

    st.write('\n')
    # SECTION (LOCATION + BOROUGH)
    with st.container(border=True):
        st.markdown("## Location")
        col1_, col2_ = st.columns(2)
        with col1_:
            with open("shared/noise_type_donut.html", 'r', errors="ignore") as f:
                html_data_3a = f.read()
            st.components.v1.html(html_data_3a, height=DEFAULT_HEIGHT)
        with col2_:
            with open("shared/street_scatter.html", 'r', errors="ignore") as f:
                html_data_3b = f.read()
            st.components.v1.html(html_data_3b, height=DEFAULT_HEIGHT)
        st.write('\n\n')
        with st.container(border=False):
            col1, col2 = st.columns([1.5, 1])
            with col1:
                st.markdown(
                    '''    
                    * Most noise incidents that occur **outdoors** (i.e., sidewalks and parks) come from **Manhattan**
    
                    * Noise complaints within commercial buildings like **restaurants and stores** occur in **Manhattan and Brooklyn** (close to **a third of commercial complaints for both**)
    
                    * **The Bronx** holds the **top 2 streets** in noise complaints across NYC, East 230th and 231st Street with a combined total of more than 300,000 noise complaints. 
    
                    * Noise complaints in **Brooklyn** are **consistently higher** compared to other boroughs when considering the **median** across each borough's streets.
    
                    * Contrarily, **Manhattan** complaints are **consistently lower** compared to other boroughs (sans Staten Island)
    
                        * Because **Manhattan** also has the **highest amount of complaints on average**, it's nearly certain that streets like **Broadway and Amsterdam Avenue** are **extreme outliers** when compared to other streets in Manhattan. Same can be said for the **noisiest streets in other boroughs due to the noticeably differences between their averages and medians**.
    
                    '''
                )
            with col2:
                df_agg = pd.read_csv('shared/groupby_borough_noise.csv').iloc[:, 1:]
                st.dataframe(df_agg)

    st.write('\n')
    # SECTION (INCIDENT DESCRIPTION)
    with st.container(border=True, height=850):
        st.markdown('''## Description''')
        col1_, col2_ = st.columns([1,2])
        with col1_:
            with open("shared/noise_type_pie.html", 'r', errors="ignore") as f:
                html_data_4a = f.read()
            st.components.v1.html(html_data_4a, height=DEFAULT_HEIGHT)
        with col2_:
            with open("shared/description_hbar.html", 'r', errors="ignore") as f:
                html_data_4b = f.read()
            st.components.v1.html(html_data_4b, height=DEFAULT_HEIGHT)

        st.markdown(
            '''
            * Residential areas like **apartment complexes or houses** are the **most common locations for noise complaints**.

            * Incidents relating to **social gatherings** are the most **substantial reasons for complaints**, more than twice of second place. 

            * Furthermore, nearly half of these incidents occur in **residential areas** while nearly a third of incidents occur on the **street / sidewalk.**

            * **Loud banging and pounding, the second most frequent** type of incident occurs nearly **exclusively in residential places**.

            * Despite being the second most common location for noise-related incidents, only two types of incidents occur in the sidewalk/streets: **loud music/party and loud talking.**

            * **Excessive volume is also associated with the most common incident relating to vehicles**, followed by a loud helicopter at second place with nearly half the amount as the former.

            ''')

    # SECTION
    st.title("Suggestions / Next Steps")
    st.markdown(
        '''
        * Station **more workers during weekends** to offset the influx of noise complaints on those days. 

        * **Less workers at the beginning of the week** while adding more as the week goes on.

        * Prioritize **more workers on later hours** of the day and **less on early morning hours**.

        * Station workers / police **near residential areas**, preferably **within consistently noisy boroughs like Brooklyn and the Bronx and especially near outlier streets like East 230th/231st and Broadway.**

        * Focus on further **training and resources** dedicated to handling and resolving **loud social incidents** like parties, banging, and talking. 
        ''')

# update every hour
MINUTE_UPDATE = 60
# MINUTE_UPDATE = 1
st.markdown(css, unsafe_allow_html=True)
st_autorefresh(interval=MINUTE_UPDATE * 60 * 1000)
