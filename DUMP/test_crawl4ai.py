import asyncio
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig

async def main():
    browser_config = BrowserConfig()  # Default browser configuration
    run_config = CrawlerRunConfig()   # Default crawl run configuration

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(
            url="https://www.bloomberg.com/news/articles/2025-06-24/us-finding-says-strikes-had-limited-impact-on-iran-nuke-program?srnd=homepage-asia&embedded-checkout=true",
            config=run_config
        )
        print(result.markdown) 
        if result.markdown:
            md_res = result.markdown
            print("Raw MD:", md_res.raw_markdown[:300])
            print("Citations MD:", md_res.markdown_with_citations[:300])
            print("References:", md_res.references_markdown)
            if md_res.fit_markdown:
                print("Pruned text:", md_res.fit_markdown[:300])
 # Print clean markdown content

if __name__ == "__main__":
    asyncio.run(main())
