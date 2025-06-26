# main.py

from market_collector import MarketCollector


def main():
    mc = MarketCollector()
    df = mc.get_events_dataframe()
    print(df.head())

if __name__ == '__main__':
    main()