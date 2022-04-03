"""
    Python script to test the Crawly API

    Jordan Zdimirovic
"""

# Import API
import pandas
from crawly import CrawlyCrawler

# Create instance
cwlr = CrawlyCrawler(config = "config.json")

# Start it!
cwlr.start()

input()

cwlr.stop()

results: pandas.DataFrame = cwlr.obtain_results()

csv_loc = input('Where would you like the CSV? : ')

results.to_csv(csv_loc)