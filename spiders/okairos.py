import scrapy
from unidecode import unidecode

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
            monthStr = unidecode(date[2][:-1]).lower()

            month = 0
            if monthStr.startswith("ia"):
                month = 1
            elif monthStr.startswith("fe"):
                month = 2
            elif monthStr.startswith("mar"):
                month = 3
            elif monthStr.startswith("apr"):
                month = 4
            elif monthStr.startswith("mai"):
                month = 5
            elif monthStr.startswith("ioun"):
                month = 6
            elif monthStr.startswith("ioul"):
                month = 7
            elif monthStr.startswith("au"):
                month = 8
            elif monthStr.startswith("sep"):
                month = 9
            elif monthStr.startswith("ok"):
                month = 10
            elif monthStr.startswith("noe"):
                month = 11
            elif monthStr.startswith("dek"):
                month = 12

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