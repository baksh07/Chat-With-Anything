from PyPDF2 import PdfReader
from unstructured.partition.pdf import partition_pdf
from pptx import Presentation
import pandas as pd
import os
import tempfile
from typing import Union, List, Dict, Optional
import requests
from bs4 import BeautifulSoup
from docx import Document
import aspose.words as aw
# import pdf2image
# import pytesseract
# from pytesseract import Output, TesseractError
import io
import warnings
from urllib.parse import urlparse
warnings.filterwarnings('ignore')

class LoadData:
    def __init__(self):
        self.supported_formats = {
            '.pdf': self.load_pdf,
            '.ppt': self.load_ppt,
            '.pptx': self.load_ppt,
            '.docx': self.load_word,
            '.doc':self.load_word,
            '.txt':self.load_text,
            '.xlsx': self.load_excel,
            '.xls': self.load_excel,
            '.mp3': self.load_audio,
            '.wav': self.load_audio,
            '.mp4': self.load_video,
            '.avi': self.load_video,
            '.mov': self.load_video,
            '.url': self.load_url
        }
        
    def load_pdf(self, data: str, pages: Optional[List[int]] = None) -> str:
        elements = partition_pdf(
        filename=data,
        strategy="ocr_only",  
        pages=pages           
        )
        all_text = " ".join([el.text for el in elements if el.text])
        return all_text
        
    def load_ppt(self, data: str) -> str:
        print(os.path.exists(data)) 
        prs = Presentation(data)
        text = ""
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, "text"):
                    text += shape.text + "\n"
        return text
            
    
    def load_word(self, data: str) -> str:
        try:
            # Check if the input file is a .doc file
            if data.lower().endswith('.doc') and not data.lower().endswith('.docx'):
                doc = aw.Document(data)
                converted_path = data + "x"  # Converts sample.doc to sample.docx
                doc.save(converted_path)
                data = converted_path  # Update path to the new .docx file

            document = Document(data)
            text = ""
            for paragraph in document.paragraphs:
                text += paragraph.text + "\n"
            return text
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

        

    def load_excel(self, data: str, sheet_name: Optional[str] = None) -> str:
        pass

    def load_audio(self, data: str, language: str = 'en-US') -> str:
        pass

    def load_video(self, data: str, extract_audio: bool = True) -> str:
        pass

    def load_url(self, data: str) -> str:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(data, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
                
            # Get text
            text = soup.get_text(separator='\n', strip=True)
            return text
        except Exception as e:
            return f"Error loading URL: {str(e)}"

    def load_text(self, data: str) -> str:
        """Load plain text file"""
        try:
            with open(data, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            return f"Error loading text file: {str(e)}"



class DataProcessor:
    def __init__(self):
        self.loader = LoadData()
    
    def save_result_to_txt(self, identifier: str, result: str, output_dir: str = "results01"):
        """Save result to a .txt file in the specified directory."""
        os.makedirs(output_dir, exist_ok=True)
        file_name = os.path.basename(identifier).split('.')[0] + ".txt"
        save_path = os.path.join(output_dir, file_name)
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(result)

    def process_file(self, file_path: str, save_to_txt: bool = True, **kwargs) -> str:
        """Process a file based on its extension and optionally save the result to a .txt file."""
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        if ext in self.loader.supported_formats:
            result = self.loader.supported_formats[ext](file_path, **kwargs)
        else:
            result = f"Unsupported file format: {ext}"

        if save_to_txt:
            self.save_result_to_txt(file_path, result)
        return result

    def process_url(self, url: str, save_to_txt: bool = True) -> str:
        """Process a URL and optionally save the result to a .txt file."""
        result = self.loader.load_url(url)

        if save_to_txt:
            domain = urlparse(url).netloc.replace('.', '_')
            self.save_result_to_txt(domain, result)
        return result

    def batch_process(self, folder_path: str, save_to_txt: bool = True, **kwargs) -> Dict[str, str]:
        """Process multiple files at once and optionally save each result to a .txt file."""
        results = {}
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            print("Processing -", file)
            result = self.process_file(file_path, save_to_txt=save_to_txt, **kwargs)
            results[file_path] = result
        return results
    


if __name__ == "__main__":

    loader = LoadData()
    file_path = '/mnt/data/vocally_batch/UPLOAD/b1fdcbd0-0a03-4313-b0c7-0a6b5e9814ee_2025-05-26.ppt'
    result = loader.load_ppt(file_path)
    print(result)
    
    # result = processor.batch_process("test")
    # print(result)  
    