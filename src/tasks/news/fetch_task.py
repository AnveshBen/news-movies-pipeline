from airflow.models import BaseOperator
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz

class NewsFetchOperator(BaseOperator):
    def __init__(self, source: str, urls: list, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls = urls
        self.source = source

    def parse_article(self, url: str, html_content: str) -> dict:
        """Parse article content based on source"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        if self.source.lower() == 'yourstory':
            title = soup.find('h1', class_='article-title') or soup.find('h1')
            text = soup.find('div', class_='article-content') or soup.find('article')
            # Find date or default to current time
            date_elem = soup.find('time') or soup.find('meta', property='article:published_time')
            
        elif self.source.lower() == 'finshots':
            title = soup.find('h1', class_='post-title') or soup.find('h1')
            text = soup.find('div', class_='post-content') or soup.find('article')
            date_elem = soup.find('time') or soup.find('meta', property='article:published_time')
            
        else:
            self.log.warning(f"Unknown source {self.source}, using generic parsing")
            title = soup.find('h1')
            text = soup.find('article') or soup.find('main')
            date_elem = soup.find('time') or soup.find('meta', property='article:published_time')

        # Extract text content and clean it
        title_text = title.get_text().strip() if title else "No title found"
        article_text = text.get_text().strip() if text else "No content found"
        
        # Parse date or use current time
        try:
            if date_elem:
                if date_elem.get('datetime'):
                    published = date_elem['datetime']
                elif date_elem.get('content'):
                    published = date_elem['content']
                else:
                    published = date_elem.get_text()
                published = datetime.fromisoformat(published.replace('Z', '+00:00'))
            else:
                published = datetime.now(pytz.UTC)
        except (ValueError, AttributeError):
            published = datetime.now(pytz.UTC)

        return {
            "title": title_text,
            "text": article_text,
            "published": published.isoformat(),
            "url": url,
            "source": self.source
        }

    def execute(self, context):
        self.log.info(f"Fetching news articles from {self.source}")
        articles = []
        
        for url in self.urls:
            try:
                self.log.info(f"Fetching article from {url}")
                response = requests.get(url)
                response.raise_for_status()
                
                article = self.parse_article(url, response.text)
                articles.append(article)
                
                self.log.info(f"Successfully parsed article: {article['title'][:50]}...")
                
            except requests.RequestException as e:
                self.log.error(f"Failed to fetch article from {url}: {str(e)}")
            except Exception as e:
                self.log.error(f"Failed to parse article from {url}: {str(e)}")
                
        self.log.info(f"Successfully fetched {len(articles)} articles from {self.source}")
        return articles 