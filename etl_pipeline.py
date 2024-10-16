import pandas as pd
import numpy as np
import luigi
import re
import csv
from src.helper.db_connector import postgres_amazon_engine, postgres_load_engine
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Proses Extract Amazon Data
class ExtractAmazonData(luigi.Task):
    def requires(self):
        pass 

    def run(self):
        # Inisialisasi engine Postgres
        engine = postgres_amazon_engine()

        # Query untuk mengambil data dari tabel amazon_sales_data
        query = "SELECT * FROM amazon_sales_data"

        # Membaca data dari sql
        amazon_data = pd.read_sql(sql=query, con=engine)

        # Menyimpan dalam bentuk CSV
        amazon_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("/Users/user/data-eng/data/raw/extract_amazon_data.csv")

 # Proses Extract Product Data   
class ExtractProductData(luigi.Task):
    csv_file = luigi.Parameter(default="/Users/user/Downloads/ElectronicsProductsPricingData.csv")

    def run(self):
        # Membaca data dari file CSV
        product_data = pd.read_csv(self.csv_file)

        # Menyimpan ke CSV
        product_data.to_csv(self.output().path, index=False)
    
    def output(self):
        return luigi.LocalTarget("/Users/user/data-eng/data/raw/extract_product_data.csv")

# Proses Extract Mydramalist Data 
class ExtractMydramalistData(luigi.Task):
    total_pages = luigi.IntParameter(default=45)  # Total halaman yang akan discrape 

    # Fungsi untuk mengambil data review
    def scrape_reviews(self, page):
        url = f'https://mydramalist.com/18452-goblin/reviews?page={page}'
        response = requests.get(url)  
        soup = BeautifulSoup(response.text, 'html.parser') 
        reviews_data = [] 

        # Mengambil elemen ulasan
        for review in soup.find_all('div', class_='review'):
            # Mengambil informasi reviewer
            reviewer = review.find('a', class_='text-primary').text.strip() if review.find('a', class_='text-primary') else "Unknown"
            profile_link = review.find('a', class_='text-primary')['href'] if review.find('a', class_='text-primary') else "No Profile Link"
            review_date = review.find('small', class_='datetime').text.strip() if review.find('small', class_='datetime') else "No Date"
            helpful_count = review.find('div', class_='user-stats').find('b').text.strip() if review.find('div', class_='user-stats') else "0"
            overall_rating = review.find('span', class_='score').text.strip() if review.find('span', class_='score') else "0"

            # Mengambil isi review
            reviews_body = review.find('div', class_='review-body')
            review_body = reviews_body.text.strip().replace('\n', ' ') if reviews_body else "No Review Available"

            # Mengambil review yang dipotong dengan "Read More"
            read_more = review.find('p', class_='read-more')
            if read_more:
                review_body += " (Full Review Not Available)"

            # Menyimpan data review ke dalam daftar
            reviews_data.append({
                'reviewer': reviewer,
                'profile_link': profile_link,
                'review_date': review_date,
                'helpful_count': helpful_count,
                'overall_rating': overall_rating,
                'review_body': review_body 
            })

        return reviews_data  # Mengembalikan data review

    def output(self):
        return luigi.LocalTarget('/Users/user/data-eng/data/raw/extract_mydramalist_data.csv')  
    
    def run(self):
        all_reviews = []  
        
        # Mengambil review dari beberapa halaman dengan progress bar
        for page in tqdm(range(1, self.total_pages + 1), desc="Scraping Pages"):
            reviews_on_page = self.scrape_reviews(page)  
            all_reviews.extend(reviews_on_page)  

        # Mengonversi data menjadi DataFrame
        goblin_reviews = pd.DataFrame(all_reviews)

        # Menyimpan DataFrame ke file CSV
        goblin_reviews.to_csv(self.output().path, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)


# Proses validasi data untuk setiap dataset
class ValidateData(luigi.Task):

    def requires(self):
        return [ExtractAmazonData(), ExtractProductData(), ExtractMydramalistData()]
    
    def run(self):
        for idx in range(0, 3):

            # Membaca data dari task sebelumnya
            data = pd.read_csv(self.input()[idx].path)

            # Memulai data quality pipeline
            print("===== Data Quality Pipeline Start =====")
            print("")

            # Memeriksa data shape
            print("===== Check Data Shape =====")
            print("")
            print(f"Data Shape for this Data {data.shape}")

            # Memeriksa tipe data
            get_cols = data.columns

            print("")
            print("===== Check Data Types =====")
            print("")

            # Iterasi untuk setiap kolom
            for col in get_cols:
                 print(f"Column {col} has data type {data[col].dtypes}")

            # Memeriksa nilai yang hilang
            print("")
            print("===== Check Missing Values =====")
            print("")

            # Iterasi untuk setiap kolom
            for col in get_cols:
                # menghitung nilai yang hilang
                get_missing_values = (data[col].isnull().sum() * 100) / len(data)
                print(f"Columns {col} has percentages missing values {get_missing_values} %")

            print("===== Data Quality Pipeline End =====")
            print("")

            # Menyimpan data yang telah divalidasi ke CSV
            data.to_csv(self.output()[idx].path, index=False)

    def output(self):
        return [
            luigi.LocalTarget("/Users/user/data-eng/data/validate/validate_amazon_data.csv"),
            luigi.LocalTarget("/Users/user/data-eng/data/validate/validate_product_data.csv"),
            luigi.LocalTarget("/Users/user/data-eng/data/validate/validate_mydramalist_data.csv")
        ]

# Proses Transformasi Amazon Data
class TransformAmazonData(luigi.Task):
    output_file = luigi.Parameter(default='/Users/user/data-eng/data/transform/transform_amazon_data.csv')

    def requires(self):
        return ValidateData()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        transform_amazon_data = pd.read_csv(self.input()[0].path)

        # Transformasi kolom 'ratings'
        transform_amazon_data["ratings"] = (
            transform_amazon_data["ratings"]
                .replace(['Get', 'FREE'], np.nan) 
                .replace({'₹': '', ',': ''}, regex=True) 
                .fillna(0) 
                .astype(float) 
        )

        # Transformasi kolom 'no_of_ratings'
        transform_amazon_data["no_of_ratings"] = (
            pd.to_numeric(transform_amazon_data["no_of_ratings"].replace(',', '', regex=True), errors='coerce').fillna(0).astype(int)
        )

        # Transformasi kolom 'discount_price'
        transform_amazon_data["discount_price"] = (
            transform_amazon_data["discount_price"]
                .replace({'₹': '', ',': ''}, regex=True)
                .replace('', np.nan) 
                .fillna(0) 
                .astype(float)
        )

        # Transformasi kolom 'actual_price'
        transform_amazon_data["actual_price"] = (
            transform_amazon_data["actual_price"]
                .replace({'₹': '', ',': ''}, regex=True) 
                .fillna(0) 
                .astype(float) 
        )

        # Hapus kolom 'Unnamed: 0' 
        transform_amazon_data.drop(columns=['Unnamed: 0'], errors='ignore', inplace=True)

        # Menyimpan hasil transformasi ke CSV
        transform_amazon_data.to_csv(self.output().path, index=False)

# Proses Transformasi Product Data
class TransformProductData(luigi.Task):
    output_file = luigi.Parameter(default="/Users/user/data-eng/data/transform/transform_product_data.csv")

    def requires(self):
        return ValidateData() 

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Membaca data dari CSV
        transform_product_data = pd.read_csv(self.input()[1].path) 

        # Daftar kolom yang dipilih
        selected_columns = [
            'id', 'prices.amountMax', 'prices.amountMin', 'prices.availability', 'prices.condition',
            'prices.merchant', 'prices.dateSeen', 'prices.isSale', 'prices.shipping', 'brand',
            'categories', 'dateAdded', 'dateUpdated', 'primaryCategories'
        ]

        # Menyaring kolom yang dipilih
        available_columns = [col for col in selected_columns if col in transform_product_data.columns]
        transform_product_data = transform_product_data[available_columns]

        # Mengganti nama kolom untuk kemudahan
        transform_product_data.rename(columns={
            'id': 'ID', 'prices.amountMax': 'MaxPrice', 'prices.amountMin': 'MinPrice', 
            'prices.availability': 'Availability', 'prices.condition': 'Condition', 
            'prices.merchant': 'Merchant', 'prices.dateSeen': 'DateSeen',
            'prices.isSale': 'isSale', 'prices.shipping': 'Shipping', 'brand': 'Brand', 
            'categories': 'SubCategories', 'dateAdded': 'DateAdded', 
            'dateUpdated': 'DateUpdated', 'primaryCategories': 'MainCategories'
        }, inplace=True)

        # Mapping nilai kolom
        transform_product_data.replace({
            'Availability': {
                'Yes': 'In Stock', 'In Stock': 'In Stock', 'TRUE': 'In Stock', 
                'undefined': 'Sold', 'yes': 'In Stock', 'Out Of Stock': 'Sold', 
                'Special Order': 'In Stock', 'No': 'Sold', 'More on the Way': 'Sold', 
                'sold': 'Sold', 'FALSE': 'Sold', 'Retired': 'Sold', '32 available:': 'In Stock', 
                '7 available': 'In Stock'
            },
            'Condition': {
                'New': 'New', 'new': 'New', 'Seller refurbished': 'Used', 
                'Used': 'Used', 'pre-owned': 'Used', 'Refurbished': 'Used', 
                'Manufacturer refurbished': 'Used'
            },
            'isSale': {False: 'No', True: 'Yes'},
            'Shipping': {
                'Expedited': 'Charges Apply', 'Value': 'Charges Apply', 
                'Standard': 'Charges Apply', 'Free Shipping on orders 35 and up': 'Free Shipping', 
                'Free Expedited Shipping': 'Free Shipping', 
                'Free Shipping on orders 35 and up': 'Free Shipping', 
                'Free Expedited Shipping for most orders over $49': 'Free Shipping',
                'FREE': 'Free Shipping', 'Freight': 'Charges Apply', 
                'Free Shipping for this Item': 'Free Shipping', 
                'Free Standard Shipping on Orders Over $49': 'Free Shipping', 
                'Free Delivery': 'Free Shipping', 
                'Free Standard Shipping': 'Free Shipping', 
                'Shipping Charges Apply': 'Charges Apply', 
                'Free Next Day Delivery (USA)': 'Free Shipping'
            }
        }, inplace=True)

        # Mengganti nilai Shipping yang mengandung USD/CAD 
        if 'Shipping' in transform_product_data.columns:
            transform_product_data['Shipping'] = (
                transform_product_data['Shipping']
                .replace(r'.*\bUSD\b.*|.*\bCAD\b.*', 'Charges Apply', regex=True)
                .fillna(method='bfill')
            )

        # Mengganti tipe data kolom DateAdded, DateUpdated, dan DateSeen menjadi datetime
        date_columns = ['DateAdded', 'DateUpdated', 'DateSeen']
        for col in date_columns:
           transform_product_data[col] = pd.to_datetime(transform_product_data[col], errors='coerce').fillna(method='bfill')
     
        # Menyimpan hasil transformasi ke CSV
        transform_product_data.to_csv(self.output().path, index=False)  

# Proses Transformasi MyDramalist Data
class TransformMydramalistData(luigi.Task):
    output_file = luigi.Parameter(default="/Users/user/data-eng/data/transform/transform_mydramalist_data.csv")  # Jalur file CSV keluaran

    def requires(self):
        return ValidateData()  # Mengharuskan tugas validasi data sebelumnya

    def run(self):
        # Membaca data dari task sebelumnya
        transform_mydramalist_data = pd.read_csv(self.input()[2].path)  # Mengambil path dari input dan membaca CSV

        # Mengonversi kolom review_date ke format datetime
        transform_mydramalist_data['review_date'] = pd.to_datetime(transform_mydramalist_data['review_date'], errors='coerce')

        # Daftar untuk menyimpan kolom review body yang dipecah menjadi beberapa kolom
        overall_ratings = []
        story_ratings = []
        acting_ratings = []
        music_ratings = []
        rewatch_value_ratings = []
        reviews = []

        # Mengekstrak rating dan ulasan dari setiap review_body
        for review_body in transform_mydramalist_data['review_body']:
            # Menggunakan regex untuk mengekstrak rating
            overall = re.search(r'Overall\s*(\d+(\.\d+)?)', review_body, re.IGNORECASE)
            story = re.search(r'Story\s*(\d+(\.\d+)?)', review_body, re.IGNORECASE)
            acting_cast = re.search(r'Acting/Cast\s*(\d+(\.\d+)?)', review_body, re.IGNORECASE)
            music = re.search(r'Music\s*(\d+(\.\d+)?)', review_body, re.IGNORECASE)
            rewatch_value = re.search(r'Rewatch Value\s*(\d+(\.\d+)?)', review_body, re.IGNORECASE)

            # Menambahkan nilai yang diekstrak atau "N/A"
            overall_ratings.append(overall.group(1) if overall else "0")
            story_ratings.append(story.group(1) if story else "0")
            acting_ratings.append(acting_cast.group(1) if acting_cast else "0")
            music_ratings.append(music.group(1) if music else "0")
            rewatch_value_ratings.append(rewatch_value.group(1) if rewatch_value else "0")
            
            # Memecah kolom 
            review_text = re.split(r'Overall\s*\d+(\.\d+)?|Story\s*\d+(\.\d+)?|Acting/Cast\s*\d+(\.\d+)?|Music\s*\d+(\.\d+)?|Rewatch Value\s*\d+(\.\d+)?', review_body)
            reviews.append(review_text[-1].strip())  

        # Membuat kolom baru di DataFrame
        transform_mydramalist_data['overall_rating'] = overall_ratings
        transform_mydramalist_data['story_rating'] = story_ratings
        transform_mydramalist_data['acting_rating'] = acting_ratings
        transform_mydramalist_data['music_rating'] = music_ratings
        transform_mydramalist_data['rewatch_value'] = rewatch_value_ratings
        transform_mydramalist_data['reviews'] = reviews

        # Mengonversi kolom rating menjadi tipe float dan mengubah missing value menjadi 0
        rating_columns = ['overall_rating', 'story_rating', 'acting_rating', 'music_rating', 'rewatch_value']
        for col in rating_columns:
            transform_mydramalist_data[col] = pd.to_numeric(transform_mydramalist_data[col], errors='coerce').fillna(0).astype(float)

        # Menghapus kolom review_body
        transform_mydramalist_data.drop(columns=['review_body'], inplace=True)

        # Menyimpan hasil transformasi ke CSV
        transform_mydramalist_data.to_csv(self.output().path, index=False)
    
    def output(self):
        return luigi.LocalTarget(self.output_file) 

# Proses Load semua data 
class LoadData(luigi.Task):
    def requires(self):
        return [
            TransformAmazonData(),  
            TransformProductData(),
            TransformMydramalistData()
        ]

    def output(self):
        return [
            luigi.LocalTarget("/Users/user/data-eng/data/load/load_amazon_data.csv"),
            luigi.LocalTarget("/Users/user/data-eng/data/load/load_product_data.csv"),
            luigi.LocalTarget("/Users/user/data-eng/data/load/load_mydramalist_data.csv")
        ]

    def run(self):
        # Inisialisasi PostgreSQL engine
        engine = postgres_load_engine() 

        # Membaca data dari task sebelumnya
        load_amazon_data = pd.read_csv(self.input()[0].path)
        load_product_data = pd.read_csv(self.input()[1].path)
        load_mydramalist_data = pd.read_csv(self.input()[2].path)

        # Menyimpan data ke database
        load_amazon_data.to_sql("AmazonData", con=engine, if_exists="append", index=False)
        load_product_data.to_sql("ProductData", con=engine, if_exists="append", index=False)
        load_mydramalist_data.to_sql("MydramalistData", con=engine, if_exists="replace", index=False)

        # Menyimpan data ke CSV
        load_amazon_data.to_csv(self.output()[0].path, index=False)
        load_product_data.to_csv(self.output()[1].path, index=False)
        load_mydramalist_data.to_csv(self.output()[2].path, index=False)

 # Memanggil build Luigi untuk menjalankan task dalam ETL pipeline 
if __name__ == "__main__":
    luigi.build([
        ExtractAmazonData(),
        ExtractProductData(),
        ExtractMydramalistData(),
        ValidateData(),
        TransformAmazonData(),
        TransformProductData(),
        TransformMydramalistData(),
        LoadData(),
    ], local_scheduler=True)