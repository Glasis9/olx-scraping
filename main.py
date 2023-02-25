import csv
import time
from dataclasses import dataclass, fields, astuple
from multiprocessing.dummy import Pool
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


@dataclass
class Ad:
    title: str
    description: str
    price: str | int
    status: str
    url: str
    date_public: str


class OlxScraping:
    _base_url = "https://www.olx.ua/uk/"
    _url_ads = []
    _parse_categories = []

    @staticmethod
    def _write_ads_to_csv(name: str, ads: list[Ad]) -> None:
        ad_fields = [field.name for field in fields(Ad)]
        with open(f"{name}.csv", "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(ad_fields)
            writer.writerows([astuple(ad) for ad in ads])

    @staticmethod
    def _scrape_logic(page_soup: BeautifulSoup, page_url: str) -> dict[str, str | int]:
        info = {}
        try:
            if len(page_soup.select("h1")):
                info["title"] = page_soup.select_one("h1").text
                if len(page_soup.select(".css-bgzo2k")):
                    info["description"] = page_soup.select_one(".css-bgzo2k").text
                else:
                    info["description"] = page_soup.select_one(".css-b7rzo5").text

                if len(page_soup.select(".css-dcwlyx > h3")):
                    if page_soup.select_one(".css-dcwlyx > h3").text == "Безкоштовно":
                        info["price"] = page_soup.select_one(".css-dcwlyx > h3").text
                    elif " ".join(page_soup.select_one(".css-dcwlyx > h3").text.split()[-3:]) == "за 1 шт.":
                        info["price"] = page_soup.select_one(".css-dcwlyx > h3").text
                    elif page_soup.select_one(".css-dcwlyx > h3").text == "Обмін":
                        info["price"] = page_soup.select_one(".css-dcwlyx > h3").text
                    else:
                        info["price"] = "".join(page_soup.select_one(".css-dcwlyx > h3").text.split()[:-1])
                else:
                    info["price"] = "None"

                info["status"] = page_soup.select_one("p.css-b5m1rv").text
                info["url"] = page_url
                if len(page_soup.select(".css-sg1fy9 > span")):
                    info["date_public"] = page_soup.select_one(".css-sg1fy9 > span").text
                else:
                    info["date_public"] = page_soup.select_one(".css-17zq51m > span").text

            else:
                # if len(page_soup.select("h4")):
                #     info["title"] = "Страница не существует"
                #     info["description"] = "None"
                #     info["price"] = "None"
                #     info["status"] = "None"
                #     info["url"] = "None"
                #     info["date_public"] = "None"
                # else:
                info["title"] = "Це оголошення більше не доступне"
                info["description"] = "None"
                info["price"] = "None"
                info["status"] = "None"
                info["url"] = "None"
                info["date_public"] = "None"
        except AttributeError:
            info["title"] = "Це оголошення більше не доступне"
            info["description"] = "None"
            info["price"] = "None"
            info["status"] = "None"
            info["url"] = "None"
            info["date_public"] = "None"

        return info

    def _scrape_page(self, page_url: str) -> dict[str, str | int]:
        page = requests.get(page_url)
        page_soup = BeautifulSoup(page.content, "lxml")

        info = self._scrape_logic(page_soup, page_url)
        if info["title"] == "Це оголошення більше не доступне":
            page = requests.get(page_url)
            page_soup = BeautifulSoup(page.content, "lxml")
            info = self._scrape_logic(page_soup, page_url)
            if info["title"] == "Це оголошення більше не доступне":
                self._add_to_parse_categories(info)
        else:
            self._add_to_parse_categories(info)

    def _add_to_parse_categories(self, info: dict[str, str | int]) -> None:
        self._parse_categories.append(Ad(
            title=info["title"],
            description=info["description"],
            price=info["price"],
            status=info["status"],
            url=info["url"],
            date_public=info["date_public"],
        ))

    @staticmethod
    def _parse_single_page(page_soup: BeautifulSoup) -> list[str]:
        url = []
        all_ads = page_soup.select_one("#offers_table").select(".wrap")
        for ads in all_ads:
            url.append(ads.select_one("a").get("href").split("#")[0])
        return url

    def _get_url_ads(self, page_url: str) -> None:
        page = requests.get(page_url).content
        page_soup = BeautifulSoup(page, "lxml")

        self._url_ads.extend(self._parse_single_page(page_soup))

        count_of_page = int(page_soup.select(".pager > .item > a")[-1].text)
        for number_page in range(2, count_of_page + 1):
            url = urljoin(page_url, f"?page={number_page}")
            page = requests.get(url)
            page_soup = BeautifulSoup(page.content, "lxml")
            self._url_ads.extend(self._parse_single_page(page_soup))

    def _get_url_main_categories(self) -> list[str]:
        page = requests.get(self._base_url)
        page_soup = BeautifulSoup(page.content, "lxml")
        categories = page_soup.select(".li")
        main_categories = []
        for category in categories:
            main_categories.append(category.select_one("a").get("href"))
        return main_categories

    def main(self) -> None:
        start = time.perf_counter()
        print("Start get categories from main page")
        main_categories = self._get_url_main_categories()
        print(f"Complete parse {len(main_categories)} categories in {time.perf_counter() - start}")

        for name_category in main_categories:
            start = time.perf_counter()
            print("Start get ads url category")
            self._get_url_ads(name_category)

            print("Start scrape ads")
            pool = Pool(11)
            pool.map(self._scrape_page, self._url_ads)

            print("Start write in csv")
            self._write_ads_to_csv(name_category.split('/')[-2], self._parse_categories)

            print(f"Parse {len(self._parse_categories)} ads of 975")
            self._url_ads.clear()
            self._parse_categories.clear()

            print(f"Completed {name_category.split('/')[-2]} category in {time.perf_counter() - start}")


if __name__ == '__main__':
    start = time.perf_counter()
    olx = OlxScraping()
    olx.main()
    print(f"Completed all category in {time.perf_counter() - start}")
