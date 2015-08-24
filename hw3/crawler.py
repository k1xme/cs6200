
# -*- coding: utf-8 -*-
import gevent
from gevent import monkey
monkey.patch_all()
from bs4 import BeautifulSoup
import urllib2, urllib
import urlparse
import requests
import time

# Dict that holds the next available request time of the domain.
domainsWaittime = {}

def work(url):
    wait_polite(url)

    if is_html(url):
        page = get_page(url)
        update_wait(url)
        update_priority(page)
        doc = parse_page(page)
        store_doc(doc) # unblocking

    # Sleep the diff between sending request and finishing.
    #gevent.time.sleep(1)


def is_html(url):
    isHtml = False
    request = urllib2.Request(url)
    request.get_method = lambda : 'HEAD'
    
    try:
        response = urllib2.urlopen(request)
        isHtml = "text/html" in response.headers["content-type"]
    except Exception, e:
        return False
    
    return isHtml

def update_priority(page):
    pass

def update_wait(url):
    pass

def get_page(url):
    request = urllib2.Request(url)
    try:
        response = urllib2.urlopen(request)
        # response = requests.get(url)
    except Exception as e:
        raise e

    page = BeautifulSoup(response)
    remove = page.findAll(["script", "style"])
    outlinks = []

    for r in remove:
        r.extract()
    
    tags_a = page.findAll("a")

    for a in tags_a:
        link = a.get("href")
        
        if not link: continue
        link = urllib.unquote(link)
        outlinks.append(canonicalize(url, link))

    # print outlinks
    # Only printing needs to convert the encoding
    #print unicode(page.get_text()).encode("utf-8")

# @param {string} base, has to be a canonicalized url.
def canonicalize(base, url):
    r = urlparse.urlparse(url)
    path = r.path[1:] if r.path.startswith("/") else r.path
    if not r.netloc: 
        if base.endswith("/"): return urlparse.urljoin(base, path)
        return urlparse.urljoin(base+"/", path)
    
    scheme = r.scheme.lower() if r.scheme else "http"
    netloc = r.netloc.lower().split(":")[0]
    base = urlparse.urlunparse((scheme, netloc, "", "", "", ""))
    
    return urlparse.urljoin(base, path)
t1 = gevent.spawn(get_page, "http://en.wikipedia.org/wiki/Cold_War")
t2 = gevent.spawn(get_page, "http://www.historylearningsite.co.uk/coldwar.htm")
t3 = gevent.spawn(get_page, "http://en.wikipedia.org/wiki/Brezhnev_Doctrine")

gevent.joinall([t1,t2,t3])
# get_page("http://en.wikipedia.org/wiki/Cold_War")
# get_page("http://www.historylearningsite.co.uk/coldwar.html")
#get_page("http://en.wikipedia.org/wiki/Brezhnev_Doctrine")