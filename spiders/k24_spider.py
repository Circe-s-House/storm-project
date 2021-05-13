import scrapy
import json

class K24Spider(scrapy.Spider):
    name = "k24"

    def start_requests(self):
        url = 'https://gr.k24.net/ellada/peloponnisos/kairos-tripoli-66'
        yield scrapy.Request(url)

    def parse(self, response):
        for tr in response.css('table.weatherGrid').css('tr')[2:]:
            humidity = tr.css("td")[7].css("span.value::text").get()
            if not humidity:
                humidity = tr.css("td")[6].css("span.value::text").get()
            yield {
                'Ιστοσελίδα':'k24.net',
                #'Ημερομηνία':h3,
                #'Ώρα':data.css('td.hour::text').get(),
                'Θερμοκρασία':tr.css("td")[4].css("span.value::text").get(),
                #'Μποφόρ':data.css('td.wind-speed::text').get().strip(),
                #'Νεφοκάλυψη':data.css('td.cloudiness::text').get().strip(),
                #'Υγρασία':data.css('td.relative-moist::text').get()[:-1],
                'Υγρασία':humidity,
            }
