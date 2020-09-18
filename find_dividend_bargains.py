import requests
import argparse

watchlist = ['IBM/IBM',
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
          'HPQ/hp',
          'NTAP/NetApp',
          'JNPR/Juniper',
          'STX/Seagate',
          'PEP/PepsiCo',
          'PG/ProcterGamble',
          'MMM/3M',
          'TROW/TRowePriceGroup',
          'CL/Colgate-Palmolive',
          'KMB/KimberlyClarkCorp',
          'CLX/CloroxCo',
          'PFE/Pfizer',
          'GILD/GileadSciences',
          'JNJ/JohnsonJohnson',
          'KO/CocaCola',
          'BK/BankofNewYorkMellon',
          'MTB/MnTBank',
          'TRV/TravelersCompanies',
          'SYF/SynchronyFinancial',
          'PNC/PNCFinancial',
          'COP/ConocoPhillips',
          'KEY/KeyCorp',
          'RF/RegionsFinancial',
          'TSN/TysonFoods',
          'FITB/FifthThirdBancorp',
          'PRU/PrudentialFin',
          'PAYX/Paychex',
          'NUE/NucorCorp',
          'MRK/Merck',
          'K/Kellogg',
          'GIS/GeneralMills',
          'CAH/CardinalHealthInc',
          'BNS/BankofNovaScotia']

small_watchlist = ['BNS/BankofNovaScotia', 'IBM/IBM', 'STX/Seagate', 'INTC/Intel',
                    'CSCO/Cisco', 'PFE/Pfizer', 'GILD/Gilead', 'NTAP/NetApp',
                    'CAH/CardinalHealthInc', 'PRU/PrudentialFin', 'MET/MetLife']

aristocrats = ['CARR','AFL','BEN','PBCT','GD','TROW','ABBV','ADM','LOW','ATO','CAH','MDT','WBA','ALB','T','AOS','CB',
               'ED','TGT','AMCR','BDX','EXPD','ESS','HRL','JNJ','MMM','PPG','ROP','ABT','ADP','CAT','LEG','PNR','SHW',
               'SWK','CINF','DOV','FRT','NUE','OTIS','PG','XOM','APD','GPC','KMB','MKC','RTX','SPGI','WMT','CTAS','ITW',
               'CL','CLX','CVX','EMR','GWW','O','PEP','ROST','SYY','ECL','KO','MCD','VFC']

warren_buffet = ['OXY/OccidentalPetroleum', 'KHC/Kraft Heinz', 'GM/GeneralMotors', 'SU/Suncor', 'WFC/WellsFargo',
                 'STOR/STORECapital', 'PSX/Phillips66', 'UPS/UnitedParcelService', 'KO/CocaCola',
                 'QSR/RestaurantBrands', 'PNC/PNCFinancialServices', 'USB/USBancorp', 'JPM/JPMorganChase',
                 'JNJ/JohnsonJohnson', 'DAL/DeltaAirlines', 'BK/BankofNewYorkMellon', 'MTB/MnTBank',
                 'PG/ProcternGamble', 'SYF/SynchronyFinancial', 'TRV/TravelersCompanies', 'GS/GoldmanSachs',
                 'BAC/BankofAmerica']

macrotrends_top_div = ['CTL/CenturyLink', 'SPG/SimonProperty', 'CQP/CheniereEnergey', 'PPL/PPL', 'KEY/KeyCorp',
                    'LYB/LyondellBasell', 'CFG/CitizensFinancial', 'RF/RegionsFinancial', 'PFG', 'FITB', 'EIX', 'OMC',
                    'COP', 'SO', 'MET', 'TFC', 'BXP', 'EQR', 'PNC', 'EXC', 'WELL', 'C', 'EVRG', 'ETR', 'PEG', 'AGR',
                    'NUE', 'AEP', 'HAS', 'SRE', 'NTRS', 'K', 'DTE', 'CVS', 'VIAC', 'EMN', 'HIG', 'PAYX', 'GIS', 'AMTD',
                    'DFS', 'STT', 'ADM', 'SJM', 'VIACA', 'CPB', 'MXIM', 'MRK', 'LNT', 'WHR', 'AMP', 'TSN', 'MS', 'CAT',
                    'CMS', 'AMGN', 'WEC', 'BLK', 'AEE', 'LMT', 'DRE', 'CMI', 'IFF', 'XEL', 'ATO', 'CAG', 'ALL']

macrotrends_url = 'https://www.macrotrends.net/stocks/charts/'
gurufocus_url = 'https://www.gurufocus.com/term/'
yahoo_url = 'https://finance.yahoo.com/quote/'


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
    val = page.text.split(" (As of")[0].split(' ')[-1]
    try:
        f = float(val)
    except ValueError:
        val = 0
    return val


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
    page = requests.get(gurufocus_url + 'zscore/' + get_symbol(stock) + '/')
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
    page = requests.get(gurufocus_url + 'mscore/' + get_symbol(stock) + '/')
    return gf_extract_headline_rank(page)


def get_piotroski_fscore(stock):
    """
    Piotroski F-score is a number between 0 and 9 which is used to assess strength of company's financial position.
    The score is used by financial investors in order to find the best value stocks (nine being the best).
    :param stock:
    :return:
    """
    page = requests.get(gurufocus_url + 'fscore/' + get_symbol(stock) + '/')
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
    page = requests.get(gurufocus_url + 'rank_profitability/' + get_symbol(stock) + '/')
    return gf_extract_headline_rank(page)


def get_pe(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/pe-ratio')
    return mt_extract_value(page, 'PE ratio as of')


def get_mkt_cap(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/market-cap')
    return mt_extract_value(page, 'market cap as of')


def get_pfcf(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/price-fcf')
    return mt_extract_value_table(page, 'Price to FCF Ratio', 4, 1)


def get_discount(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/stock-price-history')
    latest = mt_extract_value(page, 'The latest')
    average = mt_extract_value(page, 'The average')
    return round((1 - float(latest) / float(average)) * 100, 3)


def get_dividend(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/dividend-yield-history')
    return mt_extract_value(page, 'The current dividend yield').split('%')[0]


def get_dividend_safety(stock):
    return 0


def get_profit_margin(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/profit-margins')
    return mt_extract_value(page, 'net profit margin as of').split('%')[0]


def get_avg_rev_growth(stock):
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/revenue')
    num_of_years = 5
    growth_sum = 0
    curr = 0

    for i in range(1, num_of_years + 1):
        prev = curr
        curr = int(mt_extract_value_table(page, 'Annual Revenue', 2, i).split('$')[1].replace(',', ''))
        if prev:
            growth_sum += float((prev - curr) / prev)

    return str(round(growth_sum / (num_of_years - 1), 3) * 100) + '%'


def get_name(stock):
    split = stock.split('/')
    if len(split) > 1:
        return split[1]
    else:
        return stock


def get_symbol(stock):
    split = stock.split('/')
    if len(split) > 1:
        return split[0]
    else:
        return stock


def get_payout_ratios(stock):
    """
    payout_fcf (a more accurate way to look at payout ration) should be below 70%
    payout_eps (the traditional payout ratio) should be below 60%
    :param stock:
    :return:
    """
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/price-fcf')
    fcf = float(mt_extract_value_table(page, 'TTM FCF per Share', 3, 2).split('$')[1])
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/pe-ratio')
    eps = float(mt_extract_value_table(page, 'TTM Net EPS', 3, 2).split('$')[1])
    page = requests.get(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/dividend-yield-history')
    payout = float(mt_extract_value(page, 'TTM dividend payout').split('$')[1])
    payout_fcf = round(100*(payout / fcf))
    payout_eps = round(100*(payout / eps))
    return str(payout_fcf) + '%,' + str(payout_eps) + '%'


def get_rsi(stock):
    """
    An asset is usually considered overbought when the RSI is above 70% and oversold when it is below 30%.
    :param stock:
    :return:
    """
    page = requests.get('https://www.stockrsi.com/' + get_symbol(stock).lower() + '/')
    return page.text.split('RSI:')[1].split('</td></tr>')[0].split('>')[-1]


def get_expected_growth(stock):
    page = requests.get(yahoo_url + get_symbol(stock) + '/analysis')
    return page.text.split('per annum')[1].split('%')[0].split('>')[3] + '%'


def get_past_growth(stock):
    page = requests.get(yahoo_url + get_symbol(stock) + '/analysis')
    return page.text.split('per annum')[2].split('%')[0].split('>')[3] + '%'


def get_esg_risk(stock):
    """
    Sustainalytics’ ESG Risk Ratings assess the degree to which a company’s enterprise business value is at risk driven
    by environmental, social and governance issues. The rating employs a two-dimensional framework that combines an
    assessment of a company’s exposure to industry-specific material ESG issues with an assessment of how well the
    company is managing those issues.
    The final ESG Risk Ratings scores are a measure of unmanaged risk on an absolute scale of 0-100, with a lower score
    signaling less unmanaged ESG Risk.
    :param stock:
    :return:
    """
    page = requests.get(yahoo_url + get_symbol(stock) + '/sustainability')
    return page.text.split('Total ESG Risk score')[1].split('</div>')[1].split('>')[-1]


fields = {'stock': get_name,
          'mkt_cap': get_mkt_cap,
          'discount': get_discount,
          'dividend': get_dividend,
          'P/E': get_pe,
          'P/FCF': get_pfcf,
          'Div/FCF,Div/EPS': get_payout_ratios,
          'bankruptcy(>2.6)': get_altman_zscore,
          'manipulator(<-2.22)': get_beneish_mscore,
          'ESG_risk': get_esg_risk,
          'profit_margin': get_profit_margin,
          'past_growth': get_past_growth,
          'exp_growth': get_expected_growth,
          'fin_strength(>8)': get_piotroski_fscore,
          'profitability(>7)': get_profitability_rank}


def scrape(stock_names):
    data = []

    for stock in stock_names:
        print("Scraping {}...".format(stock))
        row = {}
        failed_scraping = False
        for field in fields.keys():

            try:
                row[field] = fields[field](stock)
            except Exception:
                print('Failed to scrape field ' + field + 'for stock' + stock)
                failed_scraping = True

        if not failed_scraping:
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
    parser.add_argument("stocks", help="e.g. watchlist / aristocrats / buffet / AVGO/Broadcom,CSCO/Cisco...")
    args = parser.parse_args()
    if args.stocks == 'aristocrats':
        printTable(scrape(aristocrats), fields)
    elif args.stocks == 'watchlist':
        printTable(scrape(watchlist), fields)
    elif args.stocks == 'buffet':
        printTable(scrape(warren_buffet), fields)
    else:
        printTable(scrape(args.stocks.split(',')), fields)