# Kalshi Market Collector

This project takes ingests all active Kalshi markets in order to provide our own user interface, to 
let us save historical market data (for closed markets, etc.), and to allow us run markets against 
events collected from news aggregators and other automated data collection services. 

Kalshi markets are delineated in the following ways:

| series        |  event                  |  market |
|---------------|-------------------------|----------------------|
| KXFEDDECISION |  KXFEDDECISION-25MAY    |  KXFEDDECISION-25MAY-H0 |
| Fed Decision  | Fed Decision - May 2025 |  Fed Decision - May 2025 - No Change |