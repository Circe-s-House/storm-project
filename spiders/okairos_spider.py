import scrapy
import json

class OkairosSpider(scrapy.Spider):
    name = "okairos"

    def start_requests(self):
        url = 'https://www.okairos.gr/%CF%84%CF%81%CE%AF%CF%80%CE%BF%CE%BB%CE%B7.html?v=%CF%89%CF%81%CE%B9%CE%B1%CE%AF%CE%B1'
        yield scrapy.Request(url)

    def parse(self, response):
        page = response.css('div.wnfp')
        i = 0
        while True:
            h3 = page.css('h3::text')[12+i].get()
            # if not h3:
            #     break
            for data in page.css('table')[i].css('tr')[1:]:
                yield {
                    'Ιστοσελίδα':'okairos.gr',
                    #'Ημερομηνία':h3,
                    #'Ώρα':data.css('td.hour::text').get(),
                    'Θερμοκρασία':data.css('td.temp').css('div::text').get()[:-1],
                    #'Μποφόρ':data.css('td.wind-speed::text').get().strip(),
                    #'Νεφοκάλυψη':data.css('td.cloudiness::text').get().strip(),
                    'Υγρασία':data.css('td.relative-moist::text').get()[:-1],
                }
            i += 1
