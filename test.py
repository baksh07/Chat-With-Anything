from myproject.runner import scrape_with_cli


url = "https://wcrcleaders.com/global-transformational-leader-2025-kamalakar-devaki-architect-of-global-digital-transformation/"
items = scrape_with_cli(url)
for item in items:
    print(item.get("text"))