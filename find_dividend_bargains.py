import requests

stocks = ['IBM/IBM', 'CSCO/Cisco', 'T/AT-T', 'AAPL/Apple', 'MSFT/Microsoft', 'INTC/Intel', 'JNPR/Juniper',
          'AVGO/Broadcom', 'AMAT/Applied-Materials', 'NTAP/NetApp', 'TXN/Texas-Instruments', 'NDAQ/nasdaq']


source_url = 'https://www.macrotrends.net/stocks/charts/'


def extract_value(page, marker):
    return page.text.split(marker)[1].split('gt;')[1].split('&')[0]


def extract_value_table(page, marker, column, row):
    """

    :param page:
    :param marker:
    :param column: 1-based
    :param row: 1-based, not including headline
    :return:
    """
    return page.text.split(marker)[1].split('</tr>')[row].split('</td>')[column - 1].split('>')[1]


def get_pe(stock):
    page = requests.get(source_url + stock + '/pe-ratio')
    return extract_value(page, 'PE ratio as of')


def get_pfcf(stock):
    page = requests.get(source_url + stock + '/price-fcf')
    return extract_value_table(page, 'Price to FCF Ratio', 4, 1)


def get_discount(stock):
    page = requests.get(source_url + stock + '/stock-price-history')
    latest = extract_value(page, 'The latest')
    average = extract_value(page, 'The average')
    return round((1 - float(latest) / float(average)) * 100, 3)


def get_dividend(stock):
    page = requests.get(source_url + stock + '/dividend-yield-history')
    return extract_value(page, 'The current dividend yield').split('%')[0]


def get_profit_margin(stock):
    page = requests.get(source_url + stock + '/profit-margins')
    return extract_value(page, 'net profit margin as of').split('%')[0]


def get_avg_rev_growth(stock):
    page = requests.get(source_url + stock + '/revenue')
    num_of_years = 5
    growth_sum = 0
    curr = 0

    for i in range(1, num_of_years + 1):
        prev = curr
        curr = int(extract_value_table(page, 'Annual Revenue', 2, i).split('$')[1].replace(',', ''))
        if prev:
            growth_sum += float((prev - curr) / prev)

    return round(growth_sum / (num_of_years - 1), 3)


def get_name(stock):
    return stock.split('/')[1]


fields = {'stock': get_name, 'discount': get_discount, 'dividend': get_dividend, 'P/E': get_pe, 'P/FCF': get_pfcf, \
          'avg_annual_growth': get_avg_rev_growth, 'net_profit_margin': get_profit_margin}


def scrape(stock_names):
    data = []

    for stock in stock_names:
        row = {}
        for field in fields.keys():
            row[field] = fields[field](stock)

        data.append(row)

    return reversed(sorted(data, key=lambda i: i['discount']))


def printTable(myDict, colList=None):
    """ Pretty print a list of dictionaries (myDict) as a dynamically sized table.
    If column names (colList) aren't specified, they will show in random order.
    Author: Thierry Husson - Use it as you want but don't blame me.
    """
    if not colList: colList = list(myDict[0].keys() if myDict else [])
    myList = [colList]  # 1st row = header
    for item in myDict: myList.append([str(item[col] if item[col] is not None else '') for col in colList])
    colSize = [max(map(len, col)) for col in zip(*myList)]
    formatStr = ' | '.join(["{{:<{}}}".format(i) for i in colSize])
    myList.insert(1, ['-' * i for i in colSize])  # Seperating line
    for item in myList: print(formatStr.format(*item))


if __name__ == '__main__':
    printTable(scrape(stocks), fields)