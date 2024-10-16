import requests  
from bs4 import BeautifulSoup 
import pandas as pd 
import csv 
from tqdm import tqdm 

# Fungsi untuk mengambil ulasan 
def scrape_reviews(page):
    # URL halaman yang ingin diambil
    url = f'https://mydramalist.com/18452-goblin/reviews?page={page}'
    
    # Melakukan permintaan GET untuk mengambil data dari URL
    response = requests.get(url)
    
    # Mem-parsing HTML menggunakan BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    reviews_data = []

    # Mencari semua elemen yang berisi review
    for review in soup.find_all('div', class_='review'):
        # Mengambil informasi reviewer
        reviewer = review.find('a', class_='text-primary').text.strip() if review.find('a', class_='text-primary') else "Unknown"
        profile_link = review.find('a', class_='text-primary')['href'] if review.find('a', class_='text-primary') else "No Profile Link"
        
        # Mengambil tanggal review
        review_date = review.find('small', class_='datetime').text.strip() if review.find('small', class_='datetime') else "No Date"
        
        # Mengambil jumlah "helpful count"
        helpful_count = review.find('div', class_='user-stats').find('b').text.strip() if review.find('div', class_='user-stats') else "0"
        
        # Mengambil rating 
        overall_rating = review.find('span', class_='score').text.strip() if review.find('span', class_='score') else "0"

        # Mengambil isi review
        reviews_body = review.find('div', class_='review-body')
        review_body = reviews_body.text.strip().replace('\n', ' ') if reviews_body else "No Review Body Available"

        # Menangani review yang dipotong dengan "Read More"
        read_more = review.find('p', class_='read-more')
        if read_more:
            review_body += " (Full review not accessible directly)"

        # Menyimpan data review ke dalam list
        reviews_data.append({
            'reviewer': reviewer,
            'profile_link': profile_link,
            'review_date': review_date,
            'helpful_count': helpful_count,
            'overall_rating': overall_rating,
            'review_body': review_body 
        })

    return reviews_data

# Daftar untuk menyimpan semua review
all_reviews = []

# Mengambil review dari beberapa halaman dengan progress bar
for page in tqdm(range(1, 46), desc="Scraping Pages"): 
    reviews_on_page = scrape_reviews(page) 
    all_reviews.extend(reviews_on_page)  

# Mengonversi data review  menjadi DataFrame
reviews_data = pd.DataFrame(all_reviews)

# Menyimpan file ke CSV
reviews_data.to_csv('goblin_reviews.csv', index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC) 