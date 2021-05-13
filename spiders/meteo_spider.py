import scrapy
import json

class MeteoSpider(scrapy.Spider):
    name = "meteo"

    def start_requests(self):
        url = 'https://www.meteo.gr/cf.cfm?city_id=36'
        yield scrapy.Request(url)

    def parse(self, response):
        date = ''
        for page in response.css('table#outerTable'):
            for td in page.css('td.temperature'):
                temp = td.css('div.tempcolorcell::text').get()
                humidity = td.css('div.tempcolorcell').css('div.visible-xs::text').get()[:-1]
                #time = td.css('td.fulltime').css('td::text').get()
                if temp:
                    yield {
                        'Ιστοσελίδα':'meteo.gr',
                        #'Ώρα':time,
                        'Θερμοκρασία':temp,
                        'Υγρασία':humidity
                    }

                # check_if_date = tr.css('td.forecastDate')
                # check_if_data = tr.css('td.fulltime')
                # if check_if_date:
                #     day = check_if_date.css('div.flleft::text').get()
                # elif check_if_data:
                #     yield {
                #         'Ημερομηνία':day,
                #         #'Ώρα':tr.css('td.fulltime').css('td'),
                #         #'Θερμοκρασία':tr.css(''),
                #         #'Υγρασία':tr.css(''),
                #         #'Μποφόρ':tr.css(''),
                #     }