# -*- coding: utf-8 -*-
import scrapy
import re
from ..items import NewHouseItem, ESFHhouse
import datetime
import re
from scrapy_redis.spiders import RedisSpider


class FangSpider(RedisSpider):
    name = 'fang'
    allowed_domains = ['fang.com']
    # start_urls = ['https://www.fang.com/SoufunFamily.htm']
    custom_settings = {"LOG_FILE": f"{name}+ "-" + {re.sub(r' |:','-',datetime.datetime.now())[:-7]}"}
    redis_key = 'redis:soufang'

    def parse(self, response):
        trs = response.xpath("//div[@class='outCont']//tr")
        province = None
        for tr in trs:
            tds = tr.xpath("./td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath(".//text()").get()
            province_text = re.sub(r"\s", "", province_text)
            if province_text:
                province = province_text
            if province == "其它":
                continue

            city_td = tds[1]
            city_links = city_td.xpath(".//a")
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()

                url_module = city_url.split(".")
                scheme = url_module[0]
                if "bj" in scheme:
                    newhouse_url = "https://newhouse.fang.com/house/s/"
                    esf_url = "https://esf.fang.com/"
                else:
                    newhouse_url = (scheme + ".newhouse" + ".".join(url_module[1:])).replace("http", "https")
                    esf_url = (scheme + ".esf." + ".".join(url_module[1:])).replace("http", "https")
                yield scrapy.Request(url=newhouse_url, callback=self.parse_newhouse, meta={"info": (province, city)})
                yield scrapy.Request(url=esf_url, callback=self.parse_esf, meta={"info": (province, city)})

    def parse_newhouse(self, response):
        province, city = response.meta.get("info")
        lis = response.xpath("//div[contains(@class,'nl_con')]/ul/li")
        for li in lis:
            name = li.xpath(".//div[@class='nlcd_name']/a/text()").get()
            if name:
                name = name.strip()
            house_type_list = li.xpath(".//div[contains(@class,'house_type')]/a/text()").getall()
            house_type_list = list(map(lambda x: re.sub(r"\s", "", x), house_type_list))
            rooms = list(filter(lambda x: x.endswith("居"), house_type_list))
            area = "".join(li.xpath(".//div[contains(@class,'house_type')]/text()").getall())
            area = re.sub(r"\s|－|/", "", area)
            address = li.xpath(".//div[@class='address']/a/@title").get()
            district_text = "".join(li.xpath(".//div[@class='address']/a//text()").getall())
            try:
                district = re.search(r".*\[(.+)\].*", district_text).group(1)
            except:
                print(district_text)
            sale = li.xpath(".//div[contains(@class,'fangyuan')]/span/text()").get()
            price = "".join(li.xpath(r".//div[@class='nhouse_price']//text()").getall())
            price = re.sub(r"\s|广告", "", price)
            origin_url = li.xpath(r".//div[@class='nlcd_name']/a/@href").get()
            origin_url = response.urljoin(origin_url)
            item = NewHouseItem(name=name, rooms=rooms, area=area, address=address,
                                district=district, sale=sale, price=price, origin_url=origin_url, province=province,
                                city=city)
            yield item
        next_url = response.xpath("//div[@class='page']//a[@class='next']/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_newhouse,
                                 meta={"info": (province, city)})

    def parse_esf(self, response):
        province, city = response.meta.get('info')
        dls = response.xpath("//div[@class='houseList']/dl")
        for dl in dls:
            item = ESFHhouse(province=province, city=city)
            name = dl.xpath(".//p[@class='mt10']/a/span/text()").get()
            infos = dl.xpath(".//p[class='mt12']/text()").getall()
            infos = list(map(lambda x: re.sub(r"\s", "", x), infos))
            for info in infos:
                if "厅" in info:
                    item["rooms"] = info
                elif "层" in info:
                    item["floor"] = info
                elif "向" in info:
                    item["toward"] = info
                else:
                    item['year'] = info.replace("建筑年代: ", "")
                item["address"] = dl.xpath(".//p[@class='mt10']/span/@title").get()
                item["area"] = dl.xpath(".//div[contains(@class,'area)]/p/text()").get()
                item["price"] = "".join(dl.xpath(".//div[@class='moreInfo']/p[1]//text()").get())
                item["until"] = "".join(dl.xpath(".//div[@class='moreInfo']/p[2]//text()").get())
                detail_url = dl.xpath(".//p[@class='title']/a/@href").get()
                item["origin_url"] = response.urljoin(detail_url)
                yield item

        next_url = response.xpath("//a[@id='PageControll_hlk_next']/@href").get()
        yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_esf,
                             meta={"info": (province, city)})
