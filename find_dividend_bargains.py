import requests
import argparse

stocks = ['IBM/IBM',
          'CSCO/Cisco',
          'T/AT-T',
          'AAPL/Apple',
          'MSFT/Microsoft',
          'INTC/Intel',
          'AVGO/Broadcom',
          'AMAT/Applied-Materials',
          'TXN/Texas-Instruments',
          'QCOM/Qualcomm',
          'ADP/ADP',
          'VZ/Verizon',
          'HPQ/hp']


macrotrends_url = 'https://www.macrotrends.net/stocks/charts/'
gurufocus_url = 'https://www.gurufocus.com/term/'


def mt_extract_value(page, marker):
    return page.text.split(marker)[1].split('gt;')[1].split('&')[0]


def mt_extract_value_table(page, marker, column, row):
    """

    :param page:
    :param marker:
    :param column: 1-based
    :param row: 1-based, not including headline
    :return:
    """
    return page.text.split(marker)[1].split('</tr>')[row].split('</td>')[column - 1].split('>')[1]


def gf_extract_headline_rank(page):
    return page.text.split(" (As of")[0].split(' ')[-1]


def get_altman_zscore(stock):
    """
    The Z-score formula for predicting bankruptcy.
    The formula may be used to predict the probability that a firm will go into bankruptcy within two years.
    Zones of discrimination:
        Z > 2.6 – "safe" zone
        1.1 < Z < 2.6 – "grey" zone
        Z < 1.1 – "distress" zone
    :param stock:
    :return:
    """
    page = requests.get(gurufocus_url + 'zscore/' + stock.split('/')[0] + '/')
    return gf_extract_headline_rank(page)


def get_beneish_mscore(stock):
    """
    The Beneish model is a statistical model that uses financial ratios calculated with accounting data of a specific
    company in order to check if it is likely (high probability) that the reported earnings of the company
    have been manipulated.
    The threshold value is −2.22:
        If M-score is less than −2.22, the company is unlikely to be a manipulator.
        If M-score is greater than −2.22, the company is likely to be a manipulator.
    :param stock:
    :return:
    """
    page = requests.get(gurufocus_url + 'mscore/' + stock.split('/')[0] + '/')
    return gf_extract_headline_rank(page)


def get_piotroski_fscore(stock):
    """
    Piotroski F-score is a number between 0 and 9 which is used to assess strength of company's financial position.
    The score is used by financial investors in order to find the best value stocks (nine being the best).
    :param stock:
    :return:
    """
    page = requests.get(gurufocus_url + 'fscore/' + stock.split('/')[0] + '/')
    return gf_extract_headline_rank(page)


def get_profitability_rank(stock):
    """
    GuruFocus Profitability Rank ranks how profitable a company is and how likely the company's business
    will stay that way. It is based on these factors:
    1. Operating Margin %
    2. Piotroski F-Score
    3. Trend of the Operating Margin % (5-year average). The company with an uptrend profit margin has a higher rank.
    4. Consistency of the profitability
    5. Predictability Rank

    The maximum rank is 10.
    A rank of 7 or higher means a higher profitability and may stay that way.
    A rank of 3 or lower indicates that the company has had trouble to make a profit.

    :param stock:
    :return:
    """
    page = requests.get(gurufocus_url + 'rank_profitability/' + stock.split('/')[0] + '/')
    return gf_extract_headline_rank(page)


def get_pe(stock):
    page = requests.get(macrotrends_url + stock + '/pe-ratio')
    return mt_extract_value(page, 'PE ratio as of')


def get_mkt_cap(stock):
    page = requests.get(macrotrends_url + stock + '/market-cap')
    return mt_extract_value(page, 'market cap as of')


def get_pfcf(stock):
    page = requests.get(macrotrends_url + stock + '/price-fcf')
    return mt_extract_value_table(page, 'Price to FCF Ratio', 4, 1)


def get_discount(stock):
    page = requests.get(macrotrends_url + stock + '/stock-price-history')
    latest = mt_extract_value(page, 'The latest')
    average = mt_extract_value(page, 'The average')
    return round((1 - float(latest) / float(average)) * 100, 3)


def get_dividend(stock):
    page = requests.get(macrotrends_url + stock + '/dividend-yield-history')
    return mt_extract_value(page, 'The current dividend yield').split('%')[0]


def get_profit_margin(stock):
    page = requests.get(macrotrends_url + stock + '/profit-margins')
    return mt_extract_value(page, 'net profit margin as of').split('%')[0]


def get_avg_rev_growth(stock):
    page = requests.get(macrotrends_url + stock + '/revenue')
    num_of_years = 5
    growth_sum = 0
    curr = 0

    for i in range(1, num_of_years + 1):
        prev = curr
        curr = int(mt_extract_value_table(page, 'Annual Revenue', 2, i).split('$')[1].replace(',', ''))
        if prev:
            growth_sum += float((prev - curr) / prev)

    return round(growth_sum / (num_of_years - 1), 3)


def get_name(stock):
    return stock.split('/')[1]


fields = {'stock': get_name,
          'discount': get_discount,
          'dividend': get_dividend,
          'P/E': get_pe,
          'P/FCF': get_pfcf,
          'avg_annual_growth': get_avg_rev_growth,
          'net_profit_margin': get_profit_margin,
          'market_cap': get_mkt_cap,
          'bankruptcy_risk(>2.6)': get_altman_zscore,
          'manipulator_mscore(<-2.22)': get_beneish_mscore,
          'financial_strength(>8)': get_piotroski_fscore,
          'profitability_rank(>7)': get_profitability_rank}


def scrape(stock_names):
    data = []

    for stock in stock_names:
        print("Scraping {}...".format(stock))
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
    parser = argparse.ArgumentParser()
    parser.add_argument("stocks", help="e.g. AVGO/Broadcom,CSCO/Cisco...")
    args = parser.parse_args()
    if args:
        printTable(scrape(args.stocks.split(',')), fields)
    else:
        printTable(scrape(stocks), fields)
