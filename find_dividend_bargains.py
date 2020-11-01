import requests
import argparse
from multiprocessing.dummy import Pool as ThreadPool
import json
import os
import time
import csv

stocks_dir = 'stocks/'
stock_file_format = stocks_dir + '{}.txt'
page_cache = {}

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
          'BNS/BankofNovaScotia',
          'AMGN/AmgenInc',
          'TCPC/BlackRockTCP',
          'PMT/PennyMacMortga',
          'OCSL/OaktreeSpecLend',
          'ABR/ArborRealty',
          'UNM/UnumGroup',
          'CCOI/CogentCommHold',
          'GBX/Greenbrier',
          'PFG',
          'CVS',
          'MET',
          'EQR',
          'OMC']

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

macrotrends_top_div = ['LUMN/CenturyLink', 'SPG/SimonProperty', 'CQP/CheniereEnergey', 'PPL/PPL', 'KEY/KeyCorp',
                    'LYB/LyondellBasell', 'CFG/CitizensFinancial', 'RF/RegionsFinancial', 'PFG', 'FITB', 'EIX', 'OMC',
                    'COP', 'SO', 'MET', 'TFC', 'BXP', 'EQR', 'PNC', 'EXC', 'WELL', 'C', 'EVRG', 'ETR', 'PEG', 'AGR',
                    'NUE', 'AEP', 'HAS', 'SRE', 'NTRS', 'K', 'DTE', 'CVS', 'EMN', 'HIG', 'PAYX', 'GIS', 'AMTD',
                    'DFS', 'STT', 'ADM', 'SJM', 'VIACA', 'CPB', 'MXIM', 'MRK', 'LNT', 'WHR', 'AMP', 'TSN', 'MS', 'CAT',
                    'CMS', 'AMGN', 'WEC', 'BLK', 'AEE', 'LMT', 'DRE', 'CMI', 'IFF', 'XEL', 'ATO', 'CAG', 'ALL']

macrotrends_url = 'https://www.macrotrends.net/stocks/charts/'
gurufocus_url = 'https://www.gurufocus.com/term/'
yahoo_url = 'https://finance.yahoo.com/quote/'


def get_page(url):
    page = page_cache.get(url)
    if page is not None:
        return page
    else:
        page = requests.get(url)
        page_cache[url] = page
    return page


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


def gf_extract_headline_val(page):
    return page.text.split(" (As of")[0].split(': ')[-1].strip(' %')


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
    page = get_page(gurufocus_url + 'zscore/' + get_symbol(stock) + '/')
    val = gf_extract_headline_val(page)
    try:
        float(val)
    except ValueError:
        # zscore doesn't apply to financial companies...
        val = 'N/A'
    return val


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
    page = get_page(gurufocus_url + 'mscore/' + get_symbol(stock) + '/')
    return gf_extract_headline_val(page)


def get_piotroski_fscore(stock):
    """
    Piotroski F-score is a number between 0 and 9 which is used to assess strength of company's financial position.
    The score is used by financial investors in order to find the best value stocks (nine being the best).
    :param stock:
    :return:
    """
    page = get_page(gurufocus_url + 'fscore/' + get_symbol(stock) + '/')
    return gf_extract_headline_val(page)


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
    page = get_page(gurufocus_url + 'rank_profitability/' + get_symbol(stock) + '/')
    return gf_extract_headline_val(page)



def get_pe(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/pe-ratio')
    return mt_extract_value(page, 'PE ratio as of')


def get_mkt_cap(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/market-cap')
    return mt_extract_value(page, 'market cap as of')


def get_pfcf(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/price-fcf')
    return mt_extract_value_table(page, 'Price to FCF Ratio', 4, 1)


def get_discount(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/stock-price-history')
    latest = mt_extract_value(page, 'The latest')
    average = mt_extract_value(page, 'The average')
    return '{}%'.format(round((1 - float(latest) / float(average)) * 100, 3))


def get_dividend(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/dividend-yield-history')
    return '{}%'.format(mt_extract_value(page, 'The current dividend yield').split('%')[0])


def get_profit_margin(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/profit-margins')
    return '{}%'.format(mt_extract_value(page, 'net profit margin as of').split('%')[0])


def get_dividend_history(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/dividend-yield-history')
    return page.text.split(' - ')[1].split(' ')[0]


def get_dividend_5y_growth(stock):
    page = get_page(gurufocus_url + 'dividend_growth_5y/' + get_symbol(stock) + '/')
    return '{}%'.format(gf_extract_headline_val(page))


def get_insider_ownership(stock):
    page = get_page(gurufocus_url + 'InsiderOwnership/' + get_symbol(stock) + '/')
    return gf_extract_headline_val(page)


def get_short_percentage(stock):
    page = get_page(gurufocus_url + 'InsiderOwnership/' + get_symbol(stock) + '/')
    return '{}%'.format(page.text.split('Short Percentage of Float</a></font> is <strong>')[1].split('%')[0])


def get_financial_strength(stock):
    """
    Based on 3 factors:
    1. The debt burden that the company has as measured by its Interest Coverage (current year). The higher, the better.
    2. Debt to revenue ratio. The lower, the better.
    3. Altman Z-Score.
    The maximum rank is 10. Companies with rank 7 or higher will be unlikely to fall into distressed situations.
    Companies with rank of 3 or less are likely in financial distress.
    :param stock:
    :return:
    """
    page = get_page(gurufocus_url + 'rank_balancesheet/' + get_symbol(stock) + '/')
    return gf_extract_headline_val(page)


def get_predictability_rank(stock):
    page = get_page(gurufocus_url + 'predictability_rank/' + get_symbol(stock) + '/')
    stars = len(page.text.split('<i class="fa fa-star" aria-hidden="true"></i>')) - 1
    half_stars = len(page.text.split('<i class="fa fa-star-half-o" aria-hidden="true"></i>')) - 1
    return stars + 0.5*half_stars


def get_avg_rev_growth(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/revenue')
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


def get_ttm_payout(stock):
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/dividend-yield-history')
    return float(mt_extract_value(page, 'TTM dividend payout').split('$')[1])


def get_div_over_eps(stock):
    """
    payout_eps (the traditional payout ratio) should be below 60%
    :param stock:
    :return:
    """
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/pe-ratio')
    eps = float(mt_extract_value_table(page, 'TTM Net EPS', 3, 2).split('$')[1])
    payout = get_ttm_payout(stock)
    payout_eps = round(100*(payout / eps))
    return str(payout_eps) + '%'


def get_div_over_fcf(stock):
    """
        payout_fcf (a more accurate way to look at payout ration) should be below 70%
        :param stock:
        :return:
        """
    page = get_page(macrotrends_url + get_symbol(stock) + '/' + get_symbol(stock) + '/price-fcf')
    fcf = float(mt_extract_value_table(page, 'TTM FCF per Share', 3, 2).split('$')[1])
    payout = get_ttm_payout(stock)
    payout_fcf = round(100 * (payout / fcf))
    return str(payout_fcf) + '%'


def get_rsi(stock):
    """
    An asset is usually considered overbought when the RSI is above 70% and oversold when it is below 30%.
    :param stock:
    :return:
    """
    page = get_page('https://www.stockrsi.com/' + get_symbol(stock).lower() + '/')
    return page.text.split('RSI:')[1].split('</td></tr>')[0].split('>')[-1]


def get_expected_growth(stock):
    page = get_page(yahoo_url + get_symbol(stock) + '/analysis')
    return page.text.split('per annum')[1].split('%')[0].split('>')[3] + '%'


def get_past_growth(stock):
    page = get_page(yahoo_url + get_symbol(stock) + '/analysis')
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
    page = get_page(yahoo_url + get_symbol(stock) + '/sustainability')
    return page.text.split('Total ESG Risk score')[1].split('</div>')[1].split('>')[-1]


fields = {'stock': get_name,
          'mkt_cap': get_mkt_cap,
          'discount': get_discount,
          'dividend': get_dividend,
          'div_years': get_dividend_history,
          'div_growth': get_dividend_5y_growth,
          'P/E': get_pe,
          'P/FCF': get_pfcf,
          'Div/FCF': get_div_over_fcf,
          'Div/EPS': get_div_over_eps,
          'bankruptcy(>2.6)': get_altman_zscore,
          'manipulator(<-2.22)': get_beneish_mscore,
          'ESG_risk': get_esg_risk,
          'short': get_short_percentage,
          'insdr_owsp': get_insider_ownership,
          'profit_margin': get_profit_margin,
          'past_growth': get_past_growth,
          'exp_growth': get_expected_growth,
          'piotroski(>8)': get_piotroski_fscore,
          'fin_strength(>7)': get_financial_strength,
          'profitability(>7)': get_profitability_rank}


def read_from_local_storage(stock):
    stock_file_name = stock_file_format.format(get_symbol(stock))
    row = {}

    try:
        if (time.time() - os.path.getmtime(stock_file_name)) / 3600 > 24:
            return row

        with open(stock_file_name, 'r') as f:
            row = json.loads(f.readline())
    except Exception:
        pass

    return row


def write_to_local_storage(stock, row):
    with open(stock_file_format.format(get_symbol(stock)), 'w+') as f:
        f.write(json.dumps(row))
        f.flush()


def sanitize(val):
    if len(str(val)) > 10:
        raise ValueError
    return val


def scrape_stock(stock):
    print("Scraping {}...".format(stock))

    row = read_from_local_storage(stock)

    if len(row) > 0:
        return row

    for field in fields.keys():

        try:
            raw = fields[field](stock)
            if field != 'stock':
                row[field] = sanitize(raw)
            else:
                row[field] = raw
        except Exception:
            print('Failed to scrape field ' + field + 'for stock' + stock)
            row[field] = "N/A"

    print('Finished {}'.format(stock))
    write_to_local_storage(stock, row)

    return row


def to_float_or_neg_inf(stock_row, field):
    val = float('-inf')
    try:
        val = float(stock_row[field].split('%')[0])
    except Exception:
        try:
            val = float(stock_row[field])
        except Exception:
            pass
    return val

filter_results = True
seen_stocks = set()
def stock_filter(stock_row):

    if stock_row['stock'] in seen_stocks:
        return False
    seen_stocks.add(stock_row['stock'])

    if not filter_results:
        return True

    if to_float_or_neg_inf(stock_row, 'discount') < 0:
        print('Removing {} because its expansive'.format(stock_row['stock']))
        return False

    if to_float_or_neg_inf(stock_row, 'div_growth') < 0:
        print('Removing {} because of dividend shrink'.format(stock_row['stock']))
        return False

    """
    if to_float_or_neg_inf(stock_row, 'manipulator(<-2.22)') > -2.22:
        print('Removing {} because it seems to manipulate earnings'.format(stock_row['stock']))
        return False
    """

    if to_float_or_neg_inf(stock_row, 'Div/FCF') > 70 or to_float_or_neg_inf(stock_row, 'Div/FCF') < 0:
        print('Removing {} because its Div/FCF is too high or negative'.format(stock_row['stock']))
        return False

    if to_float_or_neg_inf(stock_row, 'exp_growth') < 0:
        print('Removing {} because its expected growth is negative'.format(stock_row['stock']))
        return False

    val = to_float_or_neg_inf(stock_row, 'fin_strength(>7)')
    if val < 4:
        print('Removing {} because its financial strength is {}'.format(stock_row['stock'], val))
        return False

    val = to_float_or_neg_inf(stock_row, 'dividend')
    if val < 2.5:
        print('Removing {} because its dividend is {}'.format(stock_row['stock'], val))
        return False

    """
    val = to_float_or_neg_inf(stock_row, 'bankruptcy(>2.6)')
    if val != float('-inf') and val < 2:
        print('Removing {} because its bankruptcy risk is {}'.format(stock_row['stock'], val))
        return False
    """

    return True


def scrape(stock_names):

    try:
        os.mkdir(stocks_dir)
    except OSError:
        pass

    pool = ThreadPool(len(stock_names))
    data = pool.map(scrape_stock, stock_names)
    data = list(filter(stock_filter, data))
    data.sort(reverse=True, key=lambda i: to_float_or_neg_inf(i, 'discount')*10 +
                                          to_float_or_neg_inf(i, 'dividend')*15 +
                                          to_float_or_neg_inf(i, 'div_years')*2 +
                                          to_float_or_neg_inf(i, 'div_growth')*5 +
                                          to_float_or_neg_inf(i, 'exp_growth')*10 +
                                          to_float_or_neg_inf(i, 'fin_strength(>7)')*10 -
                                          to_float_or_neg_inf(i, 'Div/FCF')*10 -
                                          to_float_or_neg_inf(i, 'ESG_risk')*10)
    return data


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


def write_to_csv(data, fields):
    with open('{}stocks.csv'.format(stocks_dir), mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields.keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("stocks", help="e.g. watchlist / aristocrats / buffet / all / AVGO/Broadcom,CSCO/Cisco...")
    parser.add_argument('--nofilter', action='store_false')
    args = parser.parse_args()
    data = []
    filter_results = args.nofilter
    if args.stocks == 'aristocrats':
        data = scrape(aristocrats)
    elif args.stocks == 'watchlist':
        data = scrape(watchlist)
    elif args.stocks == 'buffet':
        data = scrape(warren_buffet)
    elif args.stocks == 'all':
        data = scrape(set(watchlist + warren_buffet + macrotrends_top_div + aristocrats))
    else:
        data = scrape(args.stocks.split(','))

    printTable(data, fields)
    write_to_csv(data, fields)

