import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import Optional, Tuple
import logging
from pathlib import Path
import re


class WebScraper:
    """
    A robust web scraping utility class that handles URL validation, content extraction,
    and text processing with proper error handling and logging.

    Features:
    - URL validation and normalization
    - Customizable request headers and timeout
    - Comprehensive HTML cleaning
    - Configurable text processing
    - Proper logging and error handling
    - File system operations with Path
    """

    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
    }

    DEFAULT_TIMEOUT = 15
    DEFAULT_ENCODING = 'utf-8'

    def __init__(self,
                 headers: Optional[dict] = None,
                 timeout: int = DEFAULT_TIMEOUT,
                 encoding: str = DEFAULT_ENCODING,
                 log_level: str = 'INFO'):
        """
        Initialize the WebScraper with configuration options.

        Args:
            headers: Custom request headers
            timeout: Request timeout in seconds
            encoding: Text encoding for response
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.headers = headers or self.DEFAULT_HEADERS
        self.timeout = timeout
        self.encoding = encoding
        self._configure_logging(log_level)

    def _configure_logging(self, level: str) -> None:
        """Configure logging for the class."""
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def validate_url(url: str) -> Tuple[bool, str]:
        """
        Validate and normalize a URL.

        Args:
            url: URL to validate

        Returns:
            Tuple of (is_valid, normalized_url)
        """
        try:
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False, url

            # Ensure scheme is http or https
            if parsed.scheme not in ('http', 'https'):
                return False, url

            # Normalize URL
            normalized = parsed.geturl()
            return True, normalized

        except Exception as e:
            return False, url

    def fetch_content(self, url: str) -> Optional[str]:
        """
        Fetch HTML content from a URL with proper error handling.

        Args:
            url: Valid URL to fetch

        Returns:
            HTML content as string or None if failed
        """
        try:
            self.logger.info(f"Fetching content from: {url}")
            response = requests.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()

            # Ensure proper encoding
            if response.encoding != self.encoding:
                response.encoding = self.encoding

            return response.text

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {str(e)}")
            return None

    def clean_html(self, html: str) -> BeautifulSoup:
        """
        Clean HTML content by removing unwanted elements.

        Args:
            html: Raw HTML content

        Returns:
            BeautifulSoup object with cleaned content
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Remove unnecessary elements
            for element in soup(['script', 'style', 'nav', 'footer',
                                 'iframe', 'noscript', 'svg', 'img',
                                 'button', 'form', 'input', 'select']):
                element.decompose()

            # Remove inline styles and classes
            for tag in soup.find_all(True):
                tag.attrs = {}

            return soup

        except Exception as e:
            self.logger.error(f"HTML cleaning failed: {str(e)}")
            raise

    def extract_text(self, soup: BeautifulSoup,
                     min_line_length: int = 30,
                     remove_empty_lines: bool = True) -> str:
        """
        Extract and clean text from BeautifulSoup object.

        Args:
            soup: BeautifulSoup object
            min_line_length: Minimum line length to keep
            remove_empty_lines: Whether to remove empty lines

        Returns:
            Cleaned text content
        """
        try:
            text = soup.get_text(separator='\n', strip=True)

            # Post-processing
            lines = []
            for line in text.splitlines():
                line = line.strip()

                # Skip empty lines if configured
                if remove_empty_lines and not line:
                    continue

                # Skip short lines (likely not content)
                if len(line) < min_line_length:
                    continue

                lines.append(line)

            return '\n'.join(lines)

        except Exception as e:
            self.logger.error(f"Text extraction failed: {str(e)}")
            raise

    def save_to_file(self, content: str,
                     file_path: Optional[str] = None,
                     url: Optional[str] = None) -> Path:
        """
        Save content to a text file with automatic naming if path not provided.

        Args:
            content: Text content to save
            file_path: Optional custom file path
            url: Original URL for automatic naming

        Returns:
            Path to saved file
        """
        try:
            if file_path is None:
                if url is None:
                    raise ValueError("Either file_path or url must be provided")

                # Generate filename from URL
                parsed = urlparse(url)
                domain = parsed.netloc.replace('www.', '').split('.')[0]
                sanitized = re.sub(r'\W+', '_', parsed.path)[:50]
                file_path = f"{domain}_{sanitized or 'content'}.txt"

            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            with path.open('w', encoding=self.encoding) as f:
                if url:
                    f.write(f"Source URL: {url}\n\n")
                f.write(content)

            self.logger.info(f"Content saved to: {path.resolve()}")
            return path

        except Exception as e:
            self.logger.error(f"Failed to save file: {str(e)}")
            raise

    def extract_title(self, soup: BeautifulSoup) -> str:
        """
        Extract the title of the webpage.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Page title or empty string if not found
        """
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True)
        return ""

    def scrape(self, url: str,
               save_path: Optional[str] = None,
               min_line_length: int = 30,
               save_to_file: bool = False) -> Optional[tuple]:
        """
        Complete scraping workflow from URL to saved file.

        Args:
            url: URL to scrape
            save_path: Optional custom save path
            min_line_length: Minimum line length to keep

        Returns:
            Tuple of (text, title) or None if failed
        """
        try:
            # Validate URL
            is_valid, normalized_url = self.validate_url(url)
            if not is_valid:
                self.logger.error(f"Invalid URL: {url}")
                return None

            # Fetch content
            html = self.fetch_content(normalized_url)
            if html is None:
                return None

            # Clean HTML
            soup = self.clean_html(html)

            # Extract text
            text = self.extract_text(soup, min_line_length)
            # Extract title
            title = self.extract_title(soup)
            if save_to_file:
                self.save_to_file(text, save_path, normalized_url)
            return text, title

        except Exception as e:
            self.logger.error(f"Scraping failed: {str(e)}", exc_info=True)
            return '', ''


if __name__ == "__main__":
    # Example usage
    scraper = WebScraper(log_level='DEBUG')

    while True:
        try:
            url = input("Enter URL to scrape (or 'quit' to exit): ").strip()
            if url.lower() in ('quit', 'exit'):
                break

            result = scraper.scrape(url)
            if result:
                print(f"Successfully saved to: {result}")

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            break
        except Exception as e:
            print(f"Error: {str(e)}")