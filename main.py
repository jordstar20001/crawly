"""
    Python script to test the Crawly API

    Jordan Zdimirovic
"""

# Import API
from crawly import CrawlyCrawler

# Create instance
cwlr = CrawlyCrawler(config = "config.json")

# Start it!
cwlr.start()

input()

cwlr.stop()