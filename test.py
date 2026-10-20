from myproject.runner import scrape_with_cli


def main() -> None:
    url = (
        "https://wcrcleaders.com/global-transformational-leader-2025-"
        "kamalakar-devaki-architect-of-global-digital-transformation/"
    )
    items = scrape_with_cli(url)
    for item in items:
        print(item.get("text"))


if __name__ == "__main__":
    main()
