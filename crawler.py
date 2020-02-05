import logging
import re
from urllib.parse import urlparse
from urllib.parse import urljoin
import lxml.html
from bs4 import BeautifulSoup
import time
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
        self.downloaded_urls = set()
        self.identified_traps = set()

        self.visited_subdomains = defaultdict(int)
        self.words = defaultdict(int)

        self.url_of_max_words = ""
        self.max_words = 0

        self.url_with_most_out_links = ""
        self.most_out_links = 0


    def __del__(self):
        self.log_file.close()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        print_start = time.time()
        start = time.time()

        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            # limit output to every 30 seconds or so
            if time.time() - start > 15:
                # logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
                logger.info("Fetched: %s, Queue size: %s",self.frontier.fetched, len(self.frontier))
                start = time.time()
            # if time.time() - print_start > 10:
            #     self.create_output_file()
            #     quit()
            url_data = self.corpus.fetch_url(url)

            out_link_count = 0

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        out_link_count += 1
                else:
                    # Analytic #3b: list of identified traps
                    self.identified_traps.add(next_link)

            # Analytic #2: Valid Out-links
            if self.most_out_links < out_link_count:
                self.most_out_links = out_link_count

                if url_data["is_redirected"]:
                    self.url_with_most_out_links = url_data["final_url"]
                else:
                    self.url_with_most_out_links = url_data["url"]

        logger.info("Fetched: %s, Queue size: %s",self.frontier.fetched, len(self.frontier))

        self.create_output_file()

    def is_not_stop_word(self, token):
        return token.lower() not in {
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

    def tokenize(self, text: str) -> list:
        # O(1), as checking containment with a hash-set is.
        def is_ascii(c: str) -> bool:
            return c in {'R', 'z', 'p', 'T', 'A', 'H', 'E', 'X', '8', 'J', 'k', 'g', '1', '5', 'e', 'C', 'f', 'd', 'a',
                         'l', 'B', 'c',
                         'q', 'x', '9', 'Y', 'G', 'u', '6', 'D', '4', 'v', 'U', 't', 'i', 'Q', 'L', 'h', '2', 'Z', 'I',
                         'o', 'F', 'O',
                         'm', '0', 's', 'n', 'b', 'K', 'V', 'y', 'S', 'W', 'M', 'r', 'j', 'w', '3', 'N', 'P', '7'}

        word_list = []
        character_list = []
        for char in text:
            if is_ascii(char):
                character_list.append(char.lower())
            else:
                if len(character_list) > 0:
                    word_list.append("".join(character_list))
                    character_list = []
        if len(character_list) > 0:
            word_list.append("".join(character_list))

        return word_list

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """


        # Ban non-text/HTML type documents
        try:
            if not re.search(r"text", url_data["content_type"]):
                return []
        except TypeError as e:
            return []

        url = ""
        if url_data["is_redirected"]:
            url = url_data["final_url"]
        else:
            url = url_data["url"]

        # Analytic #3a: list of downloaded URLs
        self.downloaded_urls.add(url)


        # Analytic #1: subdomains
        self.visited_subdomains[urlparse(url).netloc] += 1

        outputLinks = []

        try:
            doc = BeautifulSoup(url_data["content"], features='lxml')
        except lxml.etree.ParserError as e:
            print(f"{type(e)} ({url_data['url']}):\n{e}", file=self.log_file)
            return outputLinks
        except ValueError as e:
            print(f"{type(e)} ({url_data['url']}):\n{e}", file=self.log_file)
            return outputLinks

        a_tags = doc.find_all('a', href=True)
        for a_tag in a_tags:
            href = a_tag["href"]
            if href == '' or href[0] != '#':
                absolute = urljoin(url, href)
                outputLinks.append(absolute)


        doc_text = doc.get_text()
        doc_words = self.tokenize(doc_text)

        # Analytic #4: Longest page in terms of words
        len_doc_words = len(doc_words)
        if self.max_words < len_doc_words:
            self.max_words = len_doc_words
            self.url_of_max_words = url


        # Analytic #5: 50 most common words
        for word in self.tokenize(doc_text):

            if self.is_not_stop_word(word):
                self.words[word] += 1

        return outputLinks

    def create_output_file(self):
        with open("subdomains.txt", "w") as file:
            sorted_subdomains = sorted(self.visited_subdomains.items(), key=lambda x: x[1], reverse=True)
            for subdomain_pair in sorted_subdomains:
                subdomain = subdomain_pair[0]
                times_visited = subdomain_pair[1]
                file.write(f"{subdomain} visited {times_visited} times\n")

        with open("most_valid_outlinks.txt", "w") as file:
            file.write(f"{self.url_with_most_out_links} has {self.most_out_links} outlinks\n")

        with open("urls_and_traps.txt", "w") as file:
            file.write("Downloaded URLs:\n")
            for url in self.downloaded_urls:
                file.write(f"{url}\n")

            file.write("\n")
            file.write("Identified traps:\n")
            for trap in self.identified_traps:
                try:
                    file.write(f"{trap}\n")
                except UnicodeEncodeError:
                    pass

        with open("most_words.txt", "w") as file:
            file.write(f"{self.url_of_max_words} has {self.max_words} words\n")

        with open("most_common_words.txt", "w") as file:
            sorted_word_frequencies = sorted(self.words.items(), key=lambda x: x[1], reverse=True)
            assert len(self.words) > 0
            for word_pair_location in range(0, 50):
                word = sorted_word_frequencies[word_pair_location][0]
                frequency = sorted_word_frequencies[word_pair_location][1]
                file.write(f"{word} was used {frequency} times\n")



    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """

        # limit how deep the url goes
        slash_count = 0
        for char in url:
            if char == "/":
                slash_count += 1

            if slash_count >= 8:
                return False


        parsed_url = urlparse(url)
        url_directories = parsed_url.path.split("/")

        url_directory_set = set()
        for directory in url_directories[:-1]:
            if directory.lower() == "files":
                return False
            # limit length of directory names
            if len(directory) > 30:
                return False

            # eliminate urls with repeated directory names
            if directory.lower() in url_directory_set:
                return False
            else:
                url_directory_set.add(directory.lower())


        # restrict the number of similar queries
        try:
            match = re.fullmatch(r"(https{0,1}:\/\/.+)\?.+", url)
            base_url = match.group(1)
            self.searched_urls[base_url] += 1

            if self.searched_urls[base_url] > 500:
                return False
        except AttributeError as e:
            pass

        try:
            match = re.fullmatch(r"(.+)#.*", url)
            base_url = match.group(1)
            if base_url in self.downloaded_urls:
                return False
            else:
                self.downloaded_urls.add(base_url)
        except AttributeError as e:
            pass

        # what was here before. NO TOUCHIE
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|mat" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            # print("TypeError for ", parsed)
            return False

