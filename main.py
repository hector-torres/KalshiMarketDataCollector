# main.py

from app.market_collector import MarketCollector


def main():
    mc = MarketCollector()
    df = mc.get_events_dataframe()

if __name__ == '__main__':
    main()