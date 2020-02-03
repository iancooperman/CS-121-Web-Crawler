import logging
import re
from urllib.parse import urlparse
from urllib.parse import urljoin
import lxml.html
from time import sleep
from collections import defaultdict

logger = logging.getLogger(__name__)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

        self.log_file = open("log.txt", "w")

        self.searched_urls = defaultdict(int)

    def __del__(self):
        self.log_file.close()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        # logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))

    def is_stop_word(self, token):
        return token.lower() in {
        'than', "you're", "hasn't", 'again', 'himself', 'was', 'where', 'who', "where's", 'my', 'him', 'against', "it's", 'it', 'before',
        'could', "i'm", 'ought', "wouldn't", 'but', 'their', "who's", "hadn't", 'off', "that's", 'same', 'down', "i'd", 'they', 'about',
        'while', 'should', 'no', "don't", 'what', 'few', "i've", 'with', "you'd", 'own', 'its', 'are', 'once', 'nor', 'ours', 'then',
        'do', "doesn't", "haven't", 'at', 'why', "you'll", 'theirs', "weren't", "she'd", 'in', 'there', 'further', "she'll", 'to',
        'very', 'herself', 'have', 'ourselves', 'into', "won't", 'during', 'here', "there's", "mustn't", 'and', 'only', 'yourself',
        'has', 'of', 'whom', 'too', 'until', "let's", "they'll", "we've", 'is', 'between', 'your', 'a', "aren't", "you've", 'does',
        'you', 'doing', 'the', "they'd", 'such', "wasn't", "here's", 'be', 'from', 'how', 'after', 'above', 'any', "she's", "isn't",
        "we'd", 'we', 'am', 'having', "couldn't", 'hers', 'not', 'below', "shan't", "we're", "he'd", "they're", 'did', "they've",
        "he's", "how's", 'because', 'which', 'more', 'on', 'under', "can't", 'me', 'would', 'themselves', 'these', "what's", "he'll",
        'itself', 'each', 'that', 'them', 'for', 'all', 'our', 'up', "why's", 'or', 'an', 'were', 'through', 'cannot', 'her', "we'll",
        'yours', "when's", 'other', 'as', 'been', 'over', 'he', 'when', 'had', 'yourselves', 'both', 'she', 'being', "didn't", 'if',
        'myself', "shouldn't", 'by', 'his', 'i', 'most', 'this', 'so', 'some', "i'll", 'out', 'those'}

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []

        # if not self.is_valid(url_data["url"]):
        #     return outputLinks

        # print(url_data["content"])

        try:
            doc = lxml.html.fromstring(url_data["content"])
        except lxml.etree.ParserError as e:
            print(f"{type(e)} ({url_data['url']}):\n{e}", file=self.log_file)
            return outputLinks
        except ValueError as e:
            print(f"{type(e)} ({url_data['url']}):\n{e}", file=self.log_file)
            return outputLinks


        hrefs = doc.xpath("//a/@href")
        for href in hrefs:
            if href == '' or href[0] != '#':
                absolute = urljoin(url_data["url"], href)
                # print(absolute)
                outputLinks.append(absolute)

        
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """


        try:
            match = re.fullmatch(r"(https{0,1}:\/\/.+)\?.+", url)
            base_url = match.group(1)
            self.searched_urls[base_url] += 1

            if self.searched_urls[base_url] > 500:
                return False
        except AttributeError as e:
            pass





        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

