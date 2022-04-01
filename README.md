# *CRAWLY*
#### Recursively obtain metrics and visualisations on all hyperlinks present on a webpage.

## How does it work?
**Crawly** is a very simple implementation of a URL crawler.

All it does at its core is:
- Recursively span across all urls in the page, whilst:
    - Obtaining interesting metrics (IP location, frequency, filetype, server type, etc)
    - Attempting to be performant by using multiple threads
    - **Support for multiple pages at once**
    - Options include:
        - Gaining stats over each site
- Provide filtering options, such as:
    - Domain filtering (regex, previously seen)
    - Country filtering
    - Limits (cardinality)
- Cater for visualisations, such as:
    - Geolocational visualisations
    - Frequency visualisations
    - Network visualisations
    - More!
- Caching
    - Specific searching using precached sites

## Dependencies
#### What do you need to run *CRAWLY* for yourself?
The dependencies for this project include:
- `requests` - for obtaining the HTTP data
- `pandas` / `numpy` - for extracting and manipulating data
- [`ip-api.com`](https://ip-api.com/) - for obtaining geolocations on IPs
    - 45 uses a minute will incur some throttling (this will be taken into account, i.e. scheduling)

And that's it..! Enjoy and have fun!