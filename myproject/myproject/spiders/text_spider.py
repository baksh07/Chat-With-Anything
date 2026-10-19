import scrapy

class TextSpider(scrapy.Spider):
    name = "text"
    start_urls = []  # we’ll override via command-line

    def __init__(self, url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if url:
            self.start_urls = [url]

    def parse(self, response):
        # Extract all visible text nodes that aren't in <script> or <style>
        texts = response.xpath('//body//text()[not(ancestor::script|ancestor::style)]').getall()
        page_text = ' '.join(t.strip() for t in texts if t.strip())
        yield {"url": response.url, "text": page_text}
