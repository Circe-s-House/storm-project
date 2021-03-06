import scrapy
from datetime import *

months = ['ΙΑΝΟΥΑΡΙΟΥ', 'ΦΕΒΡΟΥΑΡΙΟΥ', 'ΜΑΡΤΙΟΥ', 'ΑΠΡΙΛΙΟΥ', 'ΜΑΪΟΥ', 'ΙΟΥΝΙΟΥ',
          'ΙΟΥΛΙΟΥ', 'ΑΥΓΟΥΣΤΟΥ', 'ΣΕΠΤΕΜΒΡΙΟΥ', 'ΟΚΤΩΒΡΙΟΥ', 'ΝΟΕΜΒΡΙΟΥ',
          'ΔΕΚΕΜΒΡΙΟΥ']

class MeteoSpider(scrapy.Spider):
    name = "meteo"

    def start_requests(self):
        url = 'https://www.meteo.gr/cf.cfm?city_id=36'
        yield scrapy.Request(url)

    def parse(self, response):
        for page in response.css('table#outerTable'):
            for tr in page.css('tr'):
                date_div = tr.css('td.forecastDate').css('div.flleft')
                time_td = tr.css('td.fulltime')
                if date_div:
                    data_span = date_div.css('span.dayNumbercf')
                    day = data_span.css('::text').get()
                    month = months.index(data_span.css('span.monthNumbercf::text').get().strip()) + 1
                    date = f'{int(day):02}/{month:02}'
                elif time_td:
                    knots = tr.css('td.anemosfull').css('tr').css('td::text').get().split()[0]
                    if knots == "ΑΠΝΟΙΑ":
                        knots = 0
                    hour = time_td.css('td::text').get()
                    if hour == '00:00':
                        parsedDate = datetime.strptime(f'{date} {hour}', '%d/%m %H:%M')
                        parsedDate += timedelta(days=1)
                        date = parsedDate.strftime('%d/%m')
                    yield {
                        'Ιστοσελίδα':'meteo.gr',
                        'Ημερομηνία':date,
                        'Ώρα':hour,
                        'Θερμοκρασία':tr.css('div.tempcolorcell::text').get(),
                        'Μποφόρ':knots,
                        'Υγρασία':tr.css('td.humidity::text').get().strip()[:-1]
                    }