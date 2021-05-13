import scrapy
import json

class K24Spider(scrapy.Spider):
    name = "k24"

    def start_requests(self):
        url = 'https://gr.k24.net/ellada/peloponnisos/kairos-tripoli-66?i=1'
        yield scrapy.Request(url)

    def parse(self, response):
        for tr in response.css('table.weatherGrid').css('tr')[2:]:
            if len(tr.css("td").getall()) == 8:
                i = 0
                date = tr.css("td")[0].css("span::text").getall()[1]
            else:
                i = 1
            yield {
                'Ιστοσελίδα':'k24.net',
                'Ημερομηνία':date,
                'Ώρα':tr.css("td")[1-i].css("::text").get(),
                'Θερμοκρασία':tr.css("td")[4-i].css("span.value::text").get(),
                'Μποφόρ':tr.css("td")[5-i].css("span.value::text").get(),
                'Υγρασία':tr.css("td")[7-i].css("span.value::text").get(),
            }
