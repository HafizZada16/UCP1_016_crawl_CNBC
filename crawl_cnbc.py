import time
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from pymongo import MongoClient, InsertOne
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# --- 1. KONFIGURASI DATABASE ---
MONGO_URL = 'mongodb+srv://hafizzada15_db_user:HpxQwitet128535@cluster16.zrqnpxj.mongodb.net/' 
DB_NAME = 'scheduler_auto'
COLLECTION_NAME = 'CNBC_Sustainability_UCP1'

client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

collection.create_index("url", unique=True)

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def get_detail_worker(link):
    driver = create_driver()
    try:
        print(f"Processing: {link[:50]}...")
        driver.get(link)
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        judul = soup.find('h1')
        judul = judul.get_text(strip=True) if judul else "Tanpa Judul"
        
        tanggal = soup.find('div', class_='date')
        tanggal = tanggal.get_text(strip=True) if tanggal else None
        
        author = soup.find('div', class_='author')
        author = author.get_text(strip=True) if author else "Redaksi CNBC Indonesia"

        tag_meta = soup.find('meta', attrs={'name': 'keywords'})
        tags = tag_meta['content'] if tag_meta else None

        body = soup.find('div', class_='detail_text')
        if not body: body = soup.find('div', class_='detail-text')
        isi_berita = ' '.join([p.get_text(strip=True) for p in body.find_all('p')]) if body else ""

        thumb_meta = soup.find('meta', attrs={'property': 'og:image'})
        thumbnail = thumb_meta['content'] if thumb_meta else None

        return {
            'url': link,
            'judul': judul,
            'tanggal_publish': tanggal,
            'author': author,
            'tag_kategori': tags,
            'isi_berita': isi_berita,
            'thumbnail': thumbnail,
            'crawled_at': datetime.now()
        }
    except Exception as e:
        print(f"❌ Error detail: {e}")
        return None
    finally:
        driver.quit()

def run_scraper():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Memulai Crawl CNBC News...")
    start_time = time.time()
    
    main_driver = create_driver()
    try:
        url = "https://www.cnbcindonesia.com/search?query=Environmental+Sustainability"
        main_driver.get(url)
        time.sleep(4)
        
        soup = BeautifulSoup(main_driver.page_source, 'lxml')
        links = []
        news_section = soup.find('div', class_='group news')
        search_target = news_section if news_section else soup
        
        for a in search_target.find_all('a'):
            href = a.get('href', '')
            if 'cnbcindonesia.com/news' in href and href not in links:
                links.append(href)
        
        main_driver.quit()
        
        if not links:
            print("Tidak menemukan link berita. Cek koneksi internet.")
            return

        print(f"Ditemukan {len(links)} link. Menjalankan 4 workers paralel...")

        results = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(get_detail_worker, link) for link in links[:15]]
            for future in futures:
                data = future.result()
                if data:
                    results.append(InsertOne(data))

        if results:
            try:
                collection.bulk_write(results, ordered=False)
                print(f"✅ SUKSES: {len(results)} data diproses ke MongoDB.")
            except Exception as e:
                print(f"Selesai. Data duplikat diabaikan, data baru berhasil disimpan.")
        
    except Exception as e:
        print(f"❌ Error Utama: {e}")
    
    print(f"Total waktu eksekusi: {time.time() - start_time:.2f} detik")

if __name__ == "__main__":
    run_scraper()