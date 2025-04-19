import scrapy
from scrapy.crawler import CrawlerProcess
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re

class MetadataSpider(scrapy.Spider):
    name = "metadata_crawler"
    custom_settings = {
        'CONCURRENT_REQUESTS': 100,
        'DOWNLOAD_DELAY': 0.1,
        'RETRY_ENABLED': False,
        'USER_AGENT': 'MetadataCrawler/1.0',
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self, urls=None, *args, **kwargs):
        super(MetadataSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls if urls else []

    def parse(self, response):
        # Extract metadata
        metadata = {
            'url': response.url,
            'title': response.xpath('//title/text()').get(),
            'description': response.xpath('//meta[@name="description"]/@content').get(),
            'keywords': response.xpath('//meta[@name="keywords"]/@content').get(),
        }

        # Extract body text
        body_text = ' '.join(response.xpath('//body//text()').getall())
        body_text = re.sub(r'\s+', ' ', body_text).strip()

        # Classify topics
        topics = self.classify_topics(body_text)

        metadata['body'] = body_text[:500]  # Limit body text for brevity
        metadata['topics'] = topics

        yield metadata

    def classify_topics(self, text):
        # Predefined topics for classification
        predefined_topics = {
            'ecommerce': ['buy', 'product', 'price', 'cart', 'review'],
            'outdoors': ['camping', 'hiking', 'outdoors', 'nature', 'adventure'],
            'politics': ['government', 'policy', 'election', 'politics', 'law'],
        }

        # Vectorize text and calculate similarity
        vectorizer = CountVectorizer().fit(predefined_topics.keys())
        topic_vectors = vectorizer.transform([' '.join(words) for words in predefined_topics.values()])
        text_vector = vectorizer.transform([text])

        similarities = cosine_similarity(text_vector, topic_vectors).flatten()
        relevant_topics = [topic for topic, score in zip(predefined_topics.keys(), similarities) if score > 0.1]

        return relevant_topics


if __name__ == "__main__":
    # Example list of URLs to crawl
    urls_to_crawl = [
        "http://www.amazon.com/Cuisinart-CPT-122-Compact-2-Slice-Toaster/dp/B009GQ034C/ref=sr_1_1?s=kitchen&ie=UTF8&qid=1431620315&sr=1-1&keywords=toaster",
        "http://www.cnn.com/2013/06/10/politics/edward-snowden-profile/",
        "https://www.bestbuy.com/site/apple-airpods-pro-2nd-generation-with-usb-c-white/6525163.p",
        "https://www.walmart.com/ip/Apple-iPhone-13-128GB-Blue/110680667",
        "https://www.bbc.com/news/world-us-canada-67512786",
        "https://www.backpacker.com/skills/beginner-skills/how-to-start-hiking/",
        "https://www.rtings.com/tv/reviews/lg/c3-oled"
    ]

    process = CrawlerProcess(settings={
        'FEEDS': {
            'output.csv': {'format': 'csv', 'encoding': 'utf8'},
        },
    })

    process.crawl(MetadataSpider, urls=urls_to_crawl)
    process.start()
