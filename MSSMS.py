import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc

# SQL Server connection details
server = 'DESKTOP-8PP0U2U'
database = 'TEST'  # Name of the database you've manually created

# Establish a connection to the newly created database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database}')
cursor = conn.cursor()

# Scraping IMDb data
headers = {'Accept-Language': 'en-US,en;q=0.5'}
base_url = 'https://www.imdb.com/search/title/?title_type=feature&num_votes=25000,&sort=user_rating,desc'
start_page = 1
movies_per_page = 50
movie_data_list = []

while True:
    url = f'{base_url}&start={start_page}&ref_=adv_nxt'
    page = requests.get(url, headers=headers)
    content = BeautifulSoup(page.content, 'lxml')

    for movie in content.select('.lister-item-content'):
        try:
            # Your updated data extraction code
            rating = movie.select('.ratings-imdb-rating strong')[0].get_text() if movie.select('.ratings-imdb-rating strong') else "N/A"
            votes = movie.select('.sort-num_votes-visible span')[1]['data-value'] if movie.select('.sort-num_votes-visible span') else "N/A"
            metascore = movie.select('.ratings-metascore span')[0].get_text() if movie.select('.ratings-metascore span') else "N/A"
            title = movie.select('.lister-item-header a')[0].get_text().strip()
            year = movie.select('.lister-item-year')[0].get_text().strip('()')
            duration = movie.select('.runtime')[0].get_text() if movie.select('.runtime') else "N/A"
            gross = "N/A"
            gross_element = movie.find('span', string='Gross:')
            if gross_element:
                gross = gross_element.find_next_sibling().get_text().strip()
            director = movie.select('.text-muted .text-muted')[2].get_text().split('|')[0].strip() if len(movie.select('.text-muted .text-muted')) > 2 else "N/A"
            stars = movie.select('.text-muted .text-muted')[2].get_text().split('|')[1].strip() if len(movie.select('.text-muted .text-muted')) > 2 else "N/A"
            genre = movie.select('.genre')[0].get_text().strip()
            
            
            data = {
                "title": title,
                "year": year,
                "duration": duration,
                "gross": gross,
                "director": director,
                "stars": stars,
                "genre": genre,
                "rating": rating,
                "votes": votes,
                "metascore": metascore
            }
            movie_data_list.append(data)
        except IndexError:
            continue

    start_page += movies_per_page

    if not content.select('.lister-page-next'):
        break

# Insert data into the database
for data in movie_data_list:
    insert_query = '''
    INSERT INTO IMDB (Title, Year, Duration, Gross, Director, Stars, Genre, Rating, Votes, Metascore)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    values = (
        data["title"],
        data["year"],
        data["duration"],
        data["gross"],
        data["director"],
        data["stars"],
        data["genre"],
        data["rating"],
        data["votes"],
        data["metascore"]
    )
    cursor.execute(insert_query, values)
    conn.commit()

# Close the database connection
conn.close()

# Save data to Excel
df = pd.DataFrame(movie_data_list)
excel_file_path = 'movie_data.xlsx'
df.to_excel(excel_file_path, index=False)

print(f"Data saved to {excel_file_path}")
