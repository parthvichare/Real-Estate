import mysql.connector
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Establish a connection to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="1234",
    database="magicbrickdb"
)

cursor=conn.cursor()

cursor.execute('Select *from flats')

rows=cursor.fetchall()

data_dict=[]

for row in rows:
    data_dict.append({
        'property_id':row[0],
        'property_name':row[1],
        'title':row[2],
        'Beds':row[3],  
        'Landmark':row[4],
        'addressLocality':row[5],
        'addressRegion':row[6],
        'area_sqft':row[7],
        'balcony':row[8],
        'bathroom':row[9],
        'carpet_area':row[10],
        'facing':row[11],
        'floor':row[12],
        'furnishing':row[13],
        'image_url':row[14],
        'latitude':row[15],
        'longitude':row[16],
        'overlook':row[17],
        'parking':row[18],
        'price':row[19],
        'status':row[20],
        'super_area':row[21],
        'url':row[22],
        'amenities':row[23],
        'url_overview':row[24],
        'flat_details':row[25],
        'NearbyLocality':row[25],
        # 'rating':row[26]
    })

cursor.close()
conn.close()

# Now you have the fetched data stored in the 'data' variable
data= pd.DataFrame(data_dict)
print(data)

st.title('Real Estate Data Dashboard')
st.subheader('DataFrame:')
st.dataframe(data)

st.subheader('Price Distribution')

# Create a histogram using Matplotlib
fig, ax = plt.subplots()
ax.hist(data['price'], bins=20, color='skyblue', edgecolor='black')  # Adjust the number of bins as needed
ax.set_xlabel('Price')
ax.set_ylabel('Frequency')
ax.set_title('Price Distribution Histogram')

# Display the histogram in Streamlit
st.pyplot(fig)