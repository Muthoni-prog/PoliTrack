import os
import sys
import requests
import pdfplumber
import pandas as pd
from tabulate import tabulate
from img2table.document import PDF
from img2table.ocr import TesseractOCR
import re
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
import os

# This automatically finds C:\Users\<YourName>\
USER_PROFILE = os.environ.get('USERPROFILE') 

# This creates a path to the Downloads folder regardless of the username
DOWNLOADS_FOLDER = os.path.join(USER_PROFILE, "Downloads", "extractor")

# Configuration
LOCAL_DOWNLOAD_PATH = os.path.join(DOWNLOADS_FOLDER, "downloaded_gazette.pdf")
# Configure Tesseract path for Windows
TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def search_gazette_notices(keyword):
    print(f"Searching ORPP Gazette Archives for '{keyword}'...", flush=True)
    url = "https://orpp.or.ke/document-category/gazette-notice/"
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        matches = []
        for a_tag in soup.find_all('a', href=True):
            title = a_tag.text.strip()
            link = a_tag['href']
            
            if not title or "document/" not in link:
                continue
                
            title_upper = title.upper()
            keyword_upper = keyword.upper()
            
            # Filtering Logic: Only select links where title contains "Allocation" and "Fund" unless overriden.
            if "ALLOCATION" in title_upper and "FUND" in title_upper and keyword_upper in title_upper:
                # Ignore rule
                if "CHANGE OF OFFICIALS" in title_upper or "REGISTRATION" in title_upper:
                    if keyword_upper not in ["CHANGE OF OFFICIALS", "REGISTRATION"]:
                        continue
                        
                if not any(m['link'] == link for m in matches):
                    matches.append({"title": title, "link": link})
                    
        if not matches:
            print("No matching notices found.", flush=True)
            return None, None, None, None
            
        print("\nTop Matches Found:", flush=True)
        for i, match in enumerate(matches[:3]):
            print(f"[{i+1}] {match['title']}", flush=True)
            
        while True:
            choice = input(f"\nSelect a notice to process (1-{min(3, len(matches))}) or 'q' to quit: ").strip().lower()
            if choice == 'q':
                return None, None, None, None
            if choice.isdigit() and 1 <= int(choice) <= len(matches[:3]):
                selected = matches[int(choice)-1]
                break
            print("Invalid choice. Please try again.")
            
        print(f"\nFetching PDF link for: {selected['title']}")
        doc_resp = requests.get(selected['link'], headers=headers, timeout=15, verify=False)
        doc_resp.raise_for_status()
        doc_soup = BeautifulSoup(doc_resp.text, 'html.parser')
        
        pdf_link = None
        # Prioritize links that are explicitly download buttons
        for a_tag in doc_soup.find_all('a', href=True):
            if '.pdf' in a_tag['href'].lower() and ('download' in a_tag.get('class', []) or a_tag.has_attr('download') or 'dlp-download-link' in a_tag.get('class', [])):
                pdf_link = a_tag['href']
                break
                
        # Fallback to any PDF link on the page if we couldn't find a button
        if not pdf_link:
            for a_tag in doc_soup.find_all('a', href=True):
                if '.pdf' in a_tag['href'].lower() and 'strategic' not in a_tag['href'].lower():
                    pdf_link = a_tag['href']
                    break
                
        if not pdf_link:
            print("Could not find a PDF download link on the document page.")
            return None, None, None, None
            
        gn_match = re.search(r'No\.\s*(\d+)', selected['title'], re.IGNORECASE)
        gn_number = gn_match.group(1) if gn_match else "UNKNOWN"
        
        date_match = re.search(r'(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+,?\s+\d{4})', selected['title'])
        doc_date = date_match.group(1) if date_match else "Unknown Date"
        
        return pdf_link, gn_number, selected['link'], doc_date
        
    except Exception as e:
        print(f"Error searching gazette notices: {e}")
        return None, None, None, None

def download_pdf(url, local_path):
    print(f"Trying to download PDF from {url}...", flush=True)
    try:
        if not url.startswith("http"):
            print("Invalid URL.")
            return False
            
        response = requests.get(url, stream=True, timeout=15, verify=False)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download successful.")
        return True
    except Exception as e:
        print(f"Download failed: {e}")
        return False

def check_pdf_is_digital(pdf_path):
    print("Checking if PDF contains digital text and tables...")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                if page.extract_text() and len(page.extract_text().strip()) > 50:
                    tables = page.extract_tables()
                    if tables and sum(len(t) for t in tables) > 2:
                        return True
        return False
    except Exception as e:
        print(f"Error checking PDF type: {e}")
        return False

def process_raw_data(raw_data, source_ref, doc_date):
    print(f"Processing {len(raw_data)} raw rows...")
    processed = []
    
    # Noise words to filter out
    NOISE_WORDS = ["GAZETTE NOTICE", "DATED THE", "THE KENYA GAZETTE", "PUBLISHED BY", "SUPPLEMENT", "CONTENTS", "PAGE"]
    
    for row in raw_data:
        row_str = " | ".join(row)
        
        # Skip header rows and known noise
        upper_row = row_str.upper()
        if any(noise in upper_row for noise in NOISE_WORDS):
            continue
            
        if "PARTY" in upper_row or "ALLOCATED" in upper_row or "AMOUNT" in upper_row or "TOTAL" in upper_row:
            continue
            
        amount_val = None
        
        # Amount cleaning
        amount_match = re.search(r'([\d,]+\.?\d*)', row[-1])
        if amount_match:
            val = amount_match.group(1).replace(',', '')
            if len(val) >= 4 and val.replace('.', '').isdigit():
                try:
                    amount_val = float(val)
                except ValueError:
                    pass
        
        if amount_val is None and len(row) > 1:
            amount_match = re.search(r'([\d,]+\.?\d*)', row[-2])
            if amount_match:
                val = amount_match.group(1).replace(',', '')
                if len(val) >= 4 and val.replace('.', '').isdigit():
                    try:
                        amount_val = float(val)
                    except ValueError:
                        pass
                        
        party_name = ""
        for cell in row:
            # Clean up the party name: remove numbers, leading [ or |, and acronyms like ODM if they are noise
            clean_cell = re.sub(r'^[\[\|\d\.\s]+', '', cell).strip()
            clean_cell = re.sub(r'[\d,\.]', '', clean_cell).strip()
            if len(clean_cell) > len(party_name) and len(clean_cell) > 5:
                # Basic check for date-like noise
                if not any(month in clean_cell.title() for month in ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]):
                    party_name = clean_cell
                
        if amount_val is not None and party_name:
            # Final noise check: ensure amount is not just a notice number (usually small or matches typical ranges)
            if amount_val > 1000 and len(party_name) < 100:
                processed.append({
                    "Party Name": party_name,
                    "Allocation Amount (KES)": amount_val,
                    "Date of Notice": doc_date,
                    "Source Reference": source_ref
                })
            
    df = pd.DataFrame(processed)
    if not df.empty:
        df = df.drop_duplicates(subset=["Party Name", "Allocation Amount (KES)"])
        
    return df

def extract_digital_tables(pdf_path, source_ref, doc_date):
    print("PDF is digital text-based. Extracting using pdfplumber...")
    extracted_data = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        row = [str(cell).strip().replace('\n', ' ') if cell else "" for cell in row]
                        if not any(row): continue
                        extracted_data.append(row)
        return process_raw_data(extracted_data, source_ref, doc_date)
    except Exception as e:
        print(f"Error extracting digital tables: {e}")
        return pd.DataFrame()

import pytesseract
import fitz
from PIL import Image
import io

def extract_scanned_tables(pdf_path, source_ref, doc_date):
    print("PDF is scanned. Extracting using fitz (PyMuPDF) + pytesseract...")
    try:
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
        raw_data = []
        doc = fitz.open(pdf_path)
        
        # Scan more pages to be thorough (up to 8)
        for page_num in range(min(8, len(doc))):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(dpi=300)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            
            text = pytesseract.image_to_string(img)
            for line in text.split('\n'):
                line = line.strip()
                if not line or len(line) < 10: continue
                
                # Check if this line is part of the Allocation notice
                # We look for lines containing a number at the end, usually preceded by a party name
                # Regex for an amount: digits and commas, at least 4 chars long, at the end of the line
                amount_match = re.search(r'([\d,]+\.?\d*)\s*$', line)
                if amount_match:
                    amount_str = amount_match.group(1).replace('(', '') # Handle noise like '(10,548...'
                    # The part before the amount is likely the party name/acronym
                    pre_amount = line[:amount_match.start()].strip()
                    # Clean up the party name (remove pipes, leading numbers/dots)
                    party_name = re.sub(r'^[|\d\.\s]+', '', pre_amount).replace('|', '').strip()
                    
                    if party_name and len(party_name) > 3:
                        # Validate the amount_str is mostly digits
                        val_check = amount_str.replace(',', '').replace('.', '')
                        if val_check.isdigit() and len(val_check) >= 4:
                            raw_data.append([party_name, amount_str])
                            
        return process_raw_data(raw_data, source_ref, doc_date)
    except Exception as e:
        print(f"Fallback extraction failed: {e}")
        return pd.DataFrame()

def main():
    print("--- Kenyan Financial Transparency Agent ---", flush=True)
    
    keyword = input("Enter a keyword to search for (e.g. '2023'): ").strip()
    if not keyword:
        keyword = "2023"
        
    pdf_url, gn_number, source_url, doc_date = search_gazette_notices(keyword)
    
    if not pdf_url:
        print("Exiting...", flush=True)
        return
        
    print(f"\nProceeding with Gazette Notice No: {gn_number}", flush=True)
    
    output_dir = r"C:\Users\Administrator\Downloads\extractor"
    output_csv_path = os.path.join(output_dir, f"GN_{gn_number}_Data.csv")
    
    if download_pdf(pdf_url, LOCAL_DOWNLOAD_PATH):
        pdf_path_to_use = LOCAL_DOWNLOAD_PATH
    else:
        print("Download failed.")
        return
        
    is_digital = check_pdf_is_digital(pdf_path_to_use)
    
    if is_digital:
        final_df = extract_digital_tables(pdf_path_to_use, source_url, doc_date)
    else:
        final_df = extract_scanned_tables(pdf_path_to_use, source_url, doc_date)
        
    if final_df.empty:
        print("\n\nNo data was extracted.")
        return
        
    print(f"\nSuccessfully extracted {len(final_df)} records.")
    
    final_df.to_csv(output_csv_path, index=False)
    print(f"Saved dataset to: {output_csv_path}\n")
    
    total_amount = final_df['Allocation Amount (KES)'].sum()
    
    display_df = final_df.drop(columns=['Source Reference'], errors='ignore').copy()
    display_df['Allocation Amount (KES)'] = display_df['Allocation Amount (KES)'].apply(lambda x: f"{x:,.2f}")
    
    print(tabulate(display_df, headers='keys', tablefmt='grid', showindex=False))
    
    print("\n" + "="*80)
    print(f"TOTAL AMOUNT DISTRIBUTED: KES {total_amount:,.2f}")
    print("="*80)
    print(f"Source URL: {source_url}\n")

if __name__ == "__main__":
    main()
