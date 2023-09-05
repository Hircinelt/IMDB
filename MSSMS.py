import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc

# SQL Server connection details
server = 'DESKTOP-8PP0U2U'
database = 'IMDB' 

# Establish a connection to the newly created database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database}')
cursor = conn.cursor()

# Delete existing data from IMDB table
delete_query = "DELETE FROM Movies"
cursor.execute(delete_query)
conn.commit()

# Execute the procedure
procedure_query = "EXEC RevertColumns"
cursor.execute(procedure_query)
conn.commit()

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
            # Data extraction code
            rating = movie.select('.ratings-imdb-rating strong')[0].get_text() if movie.select('.ratings-imdb-rating strong') else ""
            votes = movie.select('.sort-num_votes-visible span')[1]['data-value'] if movie.select('.sort-num_votes-visible span') else ""
            metascore = movie.select('.ratings-metascore span')[0].get_text() if movie.select('.ratings-metascore span') else ""
            id = movie.select_one('.lister-item-index.unbold.text-primary').get_text(strip=True).rstrip('.')
            title = movie.select('.lister-item-header a')[0].get_text().strip()
            #title_element = movie.select_one('.lister-item-header a')
            #title = title_element.get_text().strip() + " " + title_element.find_next('span', class_='lister-item-year').get_text().strip()
            year = movie.select('.lister-item-year')[0].get_text().strip('()')
            duration = movie.select('.runtime')[0].get_text() if movie.select('.runtime') else ""
            gross = movie.find_all('span', {'name': 'nv'})[1].get('data-value', '').replace(',', '') if len(movie.find_all('span', {'name': 'nv'})) > 1 else ""
            director = movie.select_one('a[href^="/name/"]').get_text() if movie.select_one('a[href^="/name/"]') else ""
            stars_elements = movie.select('p:-soup-contains("Stars:") a')
            stars = ', '.join([star.get_text() for star in stars_elements])
            star1 = stars_elements[1].get_text() if len(stars_elements) >= 1 else ""
            second_star_element = stars_elements[2] if len(stars_elements) >= 2 else None
            star2 = second_star_element.get_text() if second_star_element else ""
            third_star_element = stars_elements[3] if len(stars_elements) >= 3 else None
            star3 = third_star_element.get_text() if third_star_element else ""
            fourth_star_element = stars_elements[4] if len(stars_elements) >= 4 else None
            star4 = fourth_star_element.get_text() if fourth_star_element else ""
            genre = movie.select('.genre')[0].get_text().strip()
            genres = movie.select('.genre')[0].get_text().strip().split(', ')
            genre1 = genres[0] if len(genres) >= 1 else ""
            genre2 = genres[1] if len(genres) >= 2 else ""
            genre3 = genres[2] if len(genres) >= 3 else ""
    
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
                "metascore": metascore,
                "star1": star1,
                "star2": star2,
                "star3": star3,
                "star4": star4,
                "genre1": genre1,
                "genre2": genre2,
                "genre3": genre3,
                "id": id
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
    INSERT INTO Movies (Title, Year, Duration, Gross, Director, Stars, Genre, Rating, Votes, Metascore, Star1, Star2, Star3, Star4, Genre1, Genre2, Genre3, ID)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        data["metascore"],
        data["star1"],
        data["star2"],
        data["star3"],
        data["star4"],
        data["genre1"],
        data["genre2"],
        data["genre3"],
        data["id"]
    )
    cursor.execute(insert_query, values)
    conn.commit()
    
# Execute the procedure
procedure_query = "EXEC UpdateColumns"
cursor.execute(procedure_query)
conn.commit()

# Close the database connection
conn.close()

# Save data to Excel
df = pd.DataFrame(movie_data_list)
excel_file_path = 'movie_data.xlsx'
df.to_excel(excel_file_path, index=False)

print(f"Data saved to {excel_file_path}")
