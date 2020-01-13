import scrapy
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timezone
from pytz import timezone
from time import strptime


class OrbitalLaunchSpider(scrapy.Spider):
    name = "orbital_launches"
    start_urls = ['https://en.wikipedia.org/wiki/2019_in_spaceflight#Orbital_launches']

    def parse(self, response):
        html_content = response.text
        soup = BeautifulSoup(html_content, "lxml")

        '''
        extract required orbital launch table based on class attribute as text data
        table_rows : rows of the table
        '''

        orbital_launch_table = soup.find("table", attrs={"class": "wikitable"})
        table_rows = orbital_launch_table.tbody.find_all("tr")

        '''
        n : total no of rows in the table 
        result : <list of lists> to store table data
        row : ith row of table
        row_data : <td> tags of a row <tr>
        i = 4 (skip table headers)
        '''

        n = len(table_rows)
        i = 4
        result = []
        table_first_headers = []
        table_second_headers = []
        outcome_index = -1

        while i < n:
            row = (table_rows[i])

            '''
            AUTOMATE TABLE HEADER EXTRACTION
            if a row has <th> tag, get first order headers
            if the row has rowspan attribute, extract subsequent table headers
            '''

            '''
            table_headers = row.findAll('th')
            if len(table_headers) > 0:
                for header in table_headers:
                    table_first_headers.append(header.text.strip())

                rowspan = int(table_headers[0]['rowspan'])

                # based on rowspan : get second list of headers
                for j in range(1, rowspan):
                    next_row = table_rows[i+j]
                    cells = next_row.findAll('td')
                    if len(cells) > 1:
                        for cell in cells:
                            table_second_headers.append(cell.text.strip())

                i += rowspan
                outcome_index = table_second_headers.index('Outcome')
                continue
            '''

            # rows apart from headers
            row_data = row.findAll('td')
            cell = row_data[0]

            # skip rows which have month names and inner navigation table based on rowspan attribute
            if cell.has_attr('colspan') and cell['colspan'] == '7':
                i += 1
                continue

            '''
            All cells of importance have rowspan tag : identifies number of payloads in following rows
            Launch vehicle identified as combination of <Rocket, Flight number, Launch Site, LSP>
            Store each date in ISO Format
            '''

            if cell.has_attr('rowspan'):
                no_payloads = int(cell['rowspan'])
                day = (cell.find('span', {"class": "nowrap"})(text=True, recursive=False))[0].strip().split(' ')

                date_isoformat = (datetime(2019, strptime(day[1], '%B').tm_mon, int(day[0]), 0, 0, 0,
                                           tzinfo=timezone('UTC'))).isoformat()

                launch_vehicle_details = [date_isoformat]

                # get launch vehicle details, given by each <td> tag in the row
                for p in range(1, len(row_data)):
                    launch_vehicle_details.append(row_data[p].text.strip())

                '''
                For each payload of launch vehicle, get copy of launch vehicle detail and read subsequent
                <td> data to get relevant details
                5th <td> tag has the 'Outcome' data
                '''

                for j in range(1, no_payloads):
                    payload = table_rows[i + j]

                    # <td> tags of <tr>
                    row_data = payload.findAll('td')

                    if len(row_data) > 1:
                        entry = launch_vehicle_details.copy()
                        for k in range(0, len(row_data)):
                            if k == 5:
                                entry.append((" ".join((row_data[k])(text=True, recursive=False))).strip())
                            else:
                                entry.append(row_data[k].text.strip())

                        result.append(entry)

                # next <tr> to examine
                i += no_payloads
                continue

            i += 1

        self.analyze_data(result)

    def analyze_data(self, result):

        table_data = pd.DataFrame(result,
                                  columns=['Date', 'Rocket', 'Flight Number', 'Launch Site', 'LSP', 'Payload',
                                           'Operator', 'Orbit', 'Function', 'Decay', 'Outcome'])

        # Count number of required outcomes per distinct launch vehicle on each day
        grouped_data = table_data.groupby(['Date', 'Rocket', 'Flight Number', 'Launch Site', 'LSP'])['Outcome'] \
            .apply(lambda x: x[(x == 'Successful') | (x == 'Operational') | (x == 'En Route')].count()).reset_index(
            name='Valid Outcomes')

        grouped_data = grouped_data.groupby(['Date'])['Valid Outcomes'].apply(lambda x: x[x >= 1].count()).reset_index(
            name='No_of_Launches')

        # generate all days of 2019 in ISO format
        launch_result = pd.DataFrame(pd.date_range(start='1-1-2019', end='12-31-2019', tz='UTC'), columns=['Date'])
        launch_result['Date'] = launch_result['Date'].apply(lambda x: str(x.isoformat()))

        # join above dataframes to get desired result
        df_merged = pd.merge(launch_result, grouped_data, on='Date', how='outer')
        df_merged['No_of_Launches'] = df_merged['No_of_Launches'].fillna(0)
        (df_merged[['Date', 'No_of_Launches']]).to_csv('output.csv', header=['date', 'value'])

