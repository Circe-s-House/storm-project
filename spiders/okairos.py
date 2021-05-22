import scrapy
import datetime

months = ['Ιανουαρίου', 'Φεβρουαρίου', 'Μαρτίου', 'Απριλίου', 'Μάι', 'Ιούν',
          'Ιουλίου', 'Αυγούστου', 'Σεπτεμβρίου', 'Οκτωβρίου', 'Νοεμβρίου',
          'Δεκεμβρίου']

class OkairosSpider(scrapy.Spider):
    name = "okairos"

    def start_requests(self):
        url = 'https://www.okairos.gr/τρίπολη.html?v=ωριαία'
        yield scrapy.Request(url)

    def parse(self, response):
        page = response.css('div.wnfp')
        i = 0
        while i < 12:
            h3 = page.css('h3::text')[12+i].get()
            date = h3.split()
            month = months.index(date[2][:-1]) + 1
            finaldate = f'{date[1]}/{month:02}'
            for data in page.css('table')[i].css('tr')[1:]:
                hour = data.css('td.hour::text').get()
                if hour == "Τώρα":
                    hour = f'{datetime.datetime.now().hour:02}:00'
                yield {
                    'Ιστοσελίδα':'okairos.gr',
                    'Ημερομηνία':finaldate,
                    'Ώρα':hour,
                    'Θερμοκρασία':data.css('td.temp').css('div::text').get()[:-1],
                    'Μποφόρ':data.css('td.wind-speed::text').get().strip(),
                    'Υγρασία':data.css('td.relative-moist::text').get()[:-1],
                }
            i += 1