"""
    Crawly - URL Crawler written in Python.

    Jordan Zdimirovic. https://github.com/jordstar20001/crawly
"""

# Import dependencies
from collections import deque
from email.policy import default
import threading
from time import sleep
from xmlrpc.client import FastMarshaller
import requests, json, pandas as pd, numpy as np
from schema import Schema, And, Use, Optional, SchemaError
from crawlog import Crawlogger
import re
import socket

URL_MATCH_REGEXP = "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
#URL_MATCH_REGEXP = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
OPTIONS_SCHEMA = Schema({
    # All source pages must have http / https, and must be strings. Must be at least one src
    "source_pages": And([str], lambda lst: len(lst) > 0 and all([map(v.startswith, ["http", "https"]) for v in lst])),

    # === OPTIONAL ===

    # Only store if domain not seen
    Optional("ignore_same_domain", default=False): bool,

    # Provide a logging file
    Optional("log_file", default=None): str,

    # Should logging show in the console?
    Optional("log_console", default=False): bool,

    # Max depth is default 0 (infinity), but must be 0 or positive
    Optional("max_depth", default=0): And(int, lambda x: x >= 0),

    # Asynchronous crawling (workers)? Value of 0 means synchronous.
    Optional("workers", default=1): And(int, lambda x: 0 < x <= 16),

    # Export data to csv?
    Optional("csv_export", default=None): str,

    # Get geolocation data?
    Optional("geolocational", default=False): bool,

    # URL buffer size, determines how many URLs can be saved for later crawling.
    # If it is left default, the buffersize will be permitted to grow.
    Optional("url_buffersize", default=None): And(int, lambda x: x > 10),

    # Option to determine if depth traversal (i.e., LIFO) will be used, or
    # breadth traversal (i.e., FIFO).
    Optional("depth_first", default=False): bool
})

# Default Crawly exception
class CrawlyException(Exception): pass

def get_default_crawly_options():
    """
        Returns the default options that must be provided to the CrawlyCrawler constructor.
    """
    options = {
        "max_depth": 100,
        "source_pages": ["https://wikipedia.com"],
        "log_file": "crawler.log"
    }

    if not OPTIONS_SCHEMA.is_valid(options):
        raise CrawlyException(
            "Default options did not match the required schema.\nPlease check the source code or raise an issue on the GitHub."
        )

    return options

class CrawlyWorker():
    """
        Performs crawling and reports back to the main crawler object.
    """
    def __init__(self, worker_id: int, host: 'CrawlyCrawler'):
        self.id = worker_id
        self.__active = True
        self.working: bool = False
        self.current_job: str = None
        self.host: 'CrawlyCrawler' = host
        self.logger = Crawlogger(
            show_in_console = host.opt("log_console"),
            fpath = host.opt("log_file"),
            name = f"Worker #{worker_id}"
        )
        
        self.log = self.logger.log

    def run(self):
        self.THREAD = threading.Thread(target = self.__T_exec)
        self.THREAD.start()

    def stop(self):
        self.__active = False
        
    def __T_exec(self):
        """
            Continually pull jobs until there are none left
        """
        self.working = True
        while self.__active:
            self.current_job = self.host.get_next_job()
            if self.current_job: self.__process()
            else: sleep(1)

        self.working = False

    def __process(self):
        # First, perform get request
        current_url, parent = self.current_job
        try:
            data = requests.get(current_url)
        except: return
        self.log("Address crawled: " + current_url)
        self.log(f"Status: {data.status_code}")

        # Perform a regexp search for other urls within the data
        # url_info = [self.host.get_url_info(url) for url in re.findall(URL_MATCH_REGEXP, data.text)]
        
        url_info = self.host.get_url_info(current_url)
        urls_found = self.host.filter_urls(re.findall(URL_MATCH_REGEXP, data.text))
        
        # Add to jobs collection
        for url in urls_found:
            self.host.store_next_job((url, current_url))

        # Get other info from the request
        request_info = {
            "status_code": data.status_code,
            "content_type": data.headers["Content-Type"] if "Content-Type" in data.headers else "unknown",
            "server_type": data.headers["Server"] if "Server" in data.headers else "unknown",
            "cardinality": len(urls_found),
            "parent": parent
        }

        request_info.update(url_info)

        # Add info to host
        self.host.results.append(request_info)

def get_domain(url):
    """
        Gets the domain from a given URL
    """
    return re.sub("/.*", "", re.sub(r"https?://", "", url))
    

def url_clean(url):
    return re.sub("\?.*", "", url)

class CrawlyCrawler():
    """
        Main class for crawler.
        Manages all things to do with crawling.
    """
    def __init__(self, **kwa):
        """
            Crawly Crawler constructor.

            Keyword Arguments:
            * *options* (``dict``) --
            Provide options similar to that generated by `get_default_crawly_options()`.
            * *config* (``str``) --
            Provide a file path to a stored option JSON formatted file.
        """
        # Both options and config are not allowed
        assert not ("options" in kwa and "config" in kwa), "Cannot provide both an options dictionary and a config file"

        if "options" in kwa:
            self.options = kwa["options"]
        
        elif "config" in kwa:
            try:
                with open(kwa["config"]) as f:
                    self.options = json.loads(f.read())
            
            except FileNotFoundError:
                raise CrawlyException(f"Config file at '{kwa['config']}' was not found.")
            
            except json.decoder.JSONDecodeError:
                raise CrawlyException(f"Config file was invalid.")
        
        else:
            self.options = get_default_crawly_options()

        try:
            self.options = OPTIONS_SCHEMA.validate(self.options)

        except SchemaError as e:
            raise CrawlyException(f"Options were not valid.\n{e}")

        self.setup()

    def get_url_info(self, url: str):
        """
            Obtains info for a given URL, as determined in `options`.
        """
        # Get domain name
        # TODO: find a more efficient way to do this
        domain = get_domain(url)
        
        if url in self.sites_seen:
            self.sites_seen[url] += 1
        else: self.sites_seen[url] = 1

        if self.opt("ignore_same_domain") and domain in self.domains_seen:
            self.domains_seen[domain] += 1
        else: self.domains_seen[domain] = 1

        self.log(f"Domain determined: {domain}")

        # Get url suffix
        res = re.search("(net|com|co|gov|biz).*", domain)
        if not res:
            res = re.search("\.(?!.*\.).*$", domain)

        suffix = res.group()

        self.log(f"Suffix determined: {suffix}")

        # Get IP address
        try:
            ip_addr = socket.gethostbyname(domain)

        except:
            ip_addr = None
        
        # TODO: add more stats
        return {
            "url": url,
            "domain": domain,
            "suffix": suffix,
            "ip": ip_addr
        }

    def filter_urls(self, urls):
        """
            Filters a list of URLs based on options.
        """
        res = []
        for url in urls:
            url = url_clean(url)
            # Get domain
            domain = get_domain(url)
            if not(url in self.sites_seen or (self.opt("ignore_same_domain") and domain in self.domains_seen)):
                res.append(url)
        return res


    def get_next_job(self):
        # Depending on options, treat as queue or stack
        if not len(self.url_jobs): return None
        return self.url_jobs.popleft() if self.opt("depth_first") else self.url_jobs.pop()

    def store_next_job(self, url: str):
        # Depending on options
        if len(self.url_jobs) == self.opt("url_buffersize"):
            raise BufferError(f"URL job buffer overflow: couldn't add URL: '{url}'.")

        self.url_jobs.append(url)

    def opt(self, name: str):
        """
            Returns the option with specified name
        """
        if name in self.options: return self.options[name]
        return None

    def setup(self):
        """
            Perform operations to setup the crawling process
        """
        # Setup logger
        self.logger = Crawlogger(
            show_in_console = self.opt("log_console"),
            fpath = self.opt("log_file"),
            insta_flush = True,
            name = "CRAWLY"
        )

        # Place to store 'resultant' data
        self.results = []

        self.domains_seen = {}
        self.sites_seen = {}

        self.domains = self.opt("")

        self.log = self.logger.log

        # Create a deque to store URLS to be processed
        self.url_jobs = deque(maxlen = self.opt("url_buffersize"))

        # Store each starting page into the URL jobs
        for page in self.opt("source_pages"):
            self.store_next_job((page, None))
            
        # Create the workers (but don't start them, yet)
        self.workers = [CrawlyWorker(i + 1, self) for i in range(self.opt("workers"))]

    def start(self):
        """
            Begin the crawling process
        """
        self.log("Crawling started")

        # Create workers
        for worker in self.workers: worker.run()

    def stop(self, wait = False):
        # Tell all workers to stop
        for worker in self.workers:
            worker.stop()

        if wait:
            while any(worker.working for worker in self.workers):
                sleep(0.1)


    def obtain_results(self):
        return pd.DataFrame.from_records(self.results)